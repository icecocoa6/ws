import Amplify, { API, graphqlOperation } from "aws-amplify";
import aws_config from "./aws-exports";
import * as queries from './graphql/queries';
import * as mutations from './graphql/mutations';
import csvSync = require('csv-parse/lib/sync');
import * as fs from 'fs';
import GoogleSpreadsheet from "google-spreadsheet"

Amplify.configure(aws_config);

// 似たようなクエリインターフェースがたくさんできるのをなんとかしたい

enum GQLType {
  Maker = "Maker",
  WaterServer = "WaterServer",
  MakerDetails = "MakerDetails",
  Rankings = "Rankings",
  Water = "Water"
}

async function search(type: GQLType, condition) {
  var result = [];
  var nextToken;
  var plural = type.endsWith("s") ? type : type + "s";
  do {
    const makers = await API.graphql(graphqlOperation(queries["list" + plural], {filter: condition, nextToken: nextToken}));
    nextToken = makers["data"]["list" + plural]["nextToken"];
    Array.prototype.push.apply(result, makers["data"]["list" + plural]["items"]);
  } while (nextToken);
  return result;
}

async function searchMakers(condition) {
  return search(GQLType.Maker, condition);
}

async function searchWaterServer(condition) {
  return search(GQLType.WaterServer, condition);
}

async function searchMakerDetails(condition) {
  return search(GQLType.MakerDetails, condition);
}

async function fetchAll(type: GQLType) {
  return search(type, null);
}

async function updateOrCreateByName(type: GQLType, entity) {
  const searchResult = await search(type, {name: {eq: entity["name"]}});
  if (searchResult.length > 1) return;

  if (searchResult.length == 0) {
    console.log("create: " + JSON.stringify(entity))
    const newMaker = await API.graphql(graphqlOperation(mutations["create" + type], {input: entity}));
  } else {
    console.log("update: " + JSON.stringify(entity))
    entity['id'] = searchResult[0]['id'];
    const newMaker = await API.graphql(graphqlOperation(mutations["update" + type], {input: entity}));
  }
}

async function updateOrCreateMaker(entity) {
  return updateOrCreateByName(GQLType.Maker, entity);
}

async function updateOrCreateWaterServer(entity) {
  return updateOrCreateByName(GQLType.WaterServer, entity);
}

async function deleteAllMakers() {
  var makers;
  var nextToken;
  do {
    makers = await API.graphql(graphqlOperation(queries.listMakers, {nextToken: nextToken}));
    nextToken = makers["data"]["listMakers"]["nextToken"];
    for (var idx in makers["data"]["listMakers"]["items"]) {
      const maker = makers["data"]["listMakers"]["items"][idx];
      const params = { id: maker["id"] };
      const oldMaker = await API.graphql(graphqlOperation(mutations.deleteMaker, {input: params}));
    }
  } while (nextToken);
}

async function updateOrCreateMakerDetails(entity) {
  const searchResult = await searchMakerDetails({maker_id: {eq: entity["maker_id"]}});
  if (searchResult.length > 1) return;

  if (searchResult.length == 0) {
    const details = await API.graphql(graphqlOperation(mutations.createMakerDetails, {input: entity}));
    await API.graphql(graphqlOperation(mutations.updateMaker, {input: {id: entity["maker_id"], maker_details_id: details["data"]["createMakerDetails"]["id"]}}))
  } else {
    entity['id'] = searchResult[0]['id'];
    const details = await API.graphql(graphqlOperation(mutations.updateMakerDetails, {input: entity}));
    await API.graphql(graphqlOperation(mutations.updateMaker, {input: {id: entity["maker_id"], maker_details_id: details["data"]["updateMakerDetails"]["id"]}}))
  }
}

// deleteAllMakers().catch(function (e) { console.log(e); });

function updateMakersFromSheet(sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          promises.push(
            updateOrCreateMaker({
              name: rows[j]["メーカー名"],
              rank: rows[j]["順位"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises)
          .then(resolve)
          .catch(reject);
      });
    });
}

function updateWaterServersFromSheet(maker_id_by_name, sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          if (!rows[j]["サーバー名"]) continue;
          promises.push(
            updateOrCreateWaterServer({
              makerId: maker_id_by_name[rows[j]['メーカー名']],
              name: rows[j]["サーバー名"],
              installation: rows[j]["タイプ"],
              cartridge: rows[j]["ボトル位置"],
              weight: rows[j]["重さ"].replace(/([0-9\.]+)kg/g, "$1"),
              electricity_bill: parseInt(rows[j]["電気代"].replace(/[^0-9]/g, ""), 10),
              sales_talk: rows[j]["特徴"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises).then(resolve).catch(reject);
      });
    });
}

function updateWaterServerImagesFromSheet(sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          if (!rows[j]["サーバー名"]) continue;
          promises.push(
            updateOrCreateWaterServer({
              name: rows[j]['サーバー名'],
              image_url: rows[j]["画像"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises).then(resolve).catch(reject);
      });
    });
}

function updateMakerImagesFromSheet(sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          if (!rows[j]["メーカー名"]) continue;
          promises.push(
            updateOrCreateMaker({
              name: rows[j]['メーカー名'],
              image_url: rows[j]["画像url"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises).then(resolve).catch(reject);
      });
    });
}

function updateWaterImagesFromSheet(sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          if (!rows[j]["水の種類"]) continue;
          promises.push(
            updateOrCreateByName(GQLType.Water, {
              name: rows[j]['水の種類'],
              image_url: rows[j]["ボトル画像"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises).then(resolve).catch(reject);
      });
    });
}

function updateWaterSalesTalksFromSheet(sheet) {
  return new Promise<void[]>((resolve, reject) => {
      sheet.getRows(function(err, rows) {
        var promises: Promise<void>[] = [];
        for (var j in rows) {
          if (!rows[j]["水の種類"]) continue;
          promises.push(
            updateOrCreateByName(GQLType.Water, {
              name: rows[j]['水の種類'],
              sales_talk: rows[j]["特徴"]
            }).catch(_ => console.log(_)));
        }
        Promise.all(promises).then(resolve).catch(reject);
      });
    });
}

function updateMakerDetailsFromSheet(maker_id_by_name, sheet) {
  return new Promise<void[]>((resolve, reject) => {
    sheet.getRows((err, rows) => {
      var promises: Promise<void>[] = [];
      var price_rankings = {}
      var water_rankings = {}
      var score_rankings = {}
      for (var j in rows) {
        if (rows[j]["メーカー名"]) {
          promises.push(
            updateOrCreateMakerDetails({
              maker_id: maker_id_by_name[rows[j]["メーカー名"]],
              monthly_charge: parseInt(rows[j]["月額料金"].replace(/[^0-9]/g, ""), 10),
              water_price: parseInt(rows[j]["水の注文価格"].replace(/[^0-9]/g, ""), 10),
              minimum_lot: rows[j]["最低ロット"],
              rental: parseInt(rows[j]["レンタル料"].replace(/[^0-9]/g, ""), 10),
              electricity_bill: parseInt(rows[j]["電気代"].replace(/[^0-9]/g, ""), 10),
              installation_cost: parseInt(rows[j]["初期費用"].replace(/[^0-9]/g, ""), 10),
              delivery_cost: parseInt(rows[j]["配送料"].replace(/[^0-9]/g, ""), 10),
              maintenance_fee: parseInt(rows[j]["メンテナンス料金"].replace(/[^0-9]/g, ""), 10),
              contract_period: rows[j]["契約期間"],
              cancellation_charge: parseInt(rows[j]["解約料"].replace(/[^0-9]/g, ""), 10),
              method_of_payment: rows[j]["支払方法"],
              delivery_method: rows[j]["方式"],
              delivery_area: rows[j]["エリア"],
              delivery_time: rows[j]["曜日時間帯"],
              delivery_cycle: rows[j]["周期"],
              score: rows[j]["評価"]
            })
          )
        }
        if (rows[j]["価格順"]) {
          price_rankings[rows[j]["価格順"]] = maker_id_by_name[rows[j]["メーカー名"]]
        }
        if (rows[j]["天然水ランキング"]) {
          water_rankings[rows[j]["天然水ランキング"]] = maker_id_by_name[rows[j]["メーカー名"]]
        }
        if (rows[j]["評価ランキング"]) {
          score_rankings[rows[j]["評価ランキング"]] = maker_id_by_name[rows[j]["メーカー名"]]
        }
      }
      console.log(price_rankings)
      promises.push(
        updateOrCreateByName(GQLType.Rankings,
        {
          name: "price_rankings",
          maker_ids: Object.keys(price_rankings).map(function (key) {return price_rankings[key]})
        }))
      promises.push(
        updateOrCreateByName(GQLType.Rankings,
        {
          name: "water_rankings",
          maker_ids: Object.keys(water_rankings).map(function (key) {return water_rankings[key]})
        }))
      promises.push(
        updateOrCreateByName(GQLType.Rankings,
        {
          name: "score_rankings",
          maker_ids: Object.keys(score_rankings).map(function (key) {return score_rankings[key]})
        }))
      Promise.all(promises.map(p => p.catch(e => console.log(e))))
        .then(resolve)
        .catch(reject)
    })
  });
}

function updateWatersFromSheet(maker_id_by_name, sheet) {
  return new Promise<void[]>((resolve, reject) => {
    sheet.getRows((err, rows) => {
      var promises: Promise<void>[] = [];
      for (var j in rows) {
        if (rows[j]["水の種類"]) {
          promises.push(
            updateOrCreateByName(GQLType.Water,
            {
              name: rows[j]["水の種類"],
              maker_id: maker_id_by_name[rows[j]["メーカー名"]],
              origin: rows[j]["採水地"],
              category: rows[j]["分類"],
              deliver_method: rows[j]["宅配方式"],
              bottle: rows[j]["ボトルタイプ"],
              volume_of_bottle: rows[j]["容量l"],
              price_of_bottle: parseInt(rows[j]["価格税込"].replace(/[^0-9]/g, ""), 10),
              price_of_half_liter: parseInt(rows[j]["価格500ml"].replace(/[^0-9]/g, ""), 10),
              duration: rows[j]["賞味期限未開封"],
              hardness: rows[j]["硬度mgl"],
              pH: rows[j]["pH"],
              calcium: rows[j]["カルシウムmg"],
              sodium: rows[j]["ナトリウムmg"],
              magnesium: rows[j]["マグネシウムmg"],
              potassium: rows[j]["カリウムmg"],
              vanadium: rows[j]["バナジウムμg"],
              silica: rows[j]["シリカmg"],
              zinc: rows[j]["亜鉛mg"],
              organogermanium: rows[j]["有機ゲルマニウムμg"],
              analysis: rows[j]["検査結果"],
              analysis_date: rows[j]["水質検査日"].replace(/\//g, "-"),
            })
          )
        }
      }
      Promise.all(promises.map(p => p.catch(e => console.log(e))))
        .then(resolve)
        .catch(reject)
    })
  });
}

async function main() {
  const sheet = new GoogleSpreadsheet("151YCqajwP56ZHhPmEYWRe3jK_mu-0FkvOkOrl_Nvb2Y");
  const credentials = require("../credentials/waterserver-eeb950016754.json");

  const makers = await fetchAll(GQLType.Maker);
  var maker_id_by_name = {};
  for (var i in makers) {
    maker_id_by_name[makers[i]['name']] = makers[i]['id'];
  }

  await new Promise<void[][]>((resolve, reject) => {
    sheet.useServiceAccountAuth(credentials, function(err){
      sheet.getInfo(function(err, data){
        var promises: Promise<void[]>[] = [];
        for(var i in data.worksheets) {
          if (data.worksheets[i].title === "メーカー一覧") {
            promises.push(updateMakersFromSheet(data.worksheets[i]));
          }

          if (data.worksheets[i].title === "2. 水情報") {
            promises.push(updateWatersFromSheet(maker_id_by_name, data.worksheets[i]));
          }

          if (data.worksheets[i].title === "1. サーバー情報") {
            promises.push(updateWaterServersFromSheet(maker_id_by_name, data.worksheets[i]));
          }

          if (data.worksheets[i].title === "ランキング生成") {
            promises.push(updateMakerDetailsFromSheet(maker_id_by_name, data.worksheets[i]))
          }

          if (data.worksheets[i].title === "ServerImage") {
            promises.push(updateWaterServerImagesFromSheet(data.worksheets[i]))
          }

          if (data.worksheets[i].title === "MakerImage") {
            promises.push(updateMakerImagesFromSheet(data.worksheets[i]))
          }

          if (data.worksheets[i].title === "Water") {
            promises.push(updateWaterImagesFromSheet(data.worksheets[i]))
            promises.push(updateWaterSalesTalksFromSheet(data.worksheets[i]))
          }
        }

        Promise.all(promises.map(p => p.catch(e => e))).then(resolve).catch(reject);
      });
    });
  });
}

main()
  .then(() => { console.log("finishing..."); process.exit(0); })
  .catch((e) => { console.log(e); process.exit(-1); });
