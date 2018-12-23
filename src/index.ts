import Amplify, { API, graphqlOperation } from "aws-amplify";
import aws_config from "./aws-exports";
import * as queries from './graphql/queries';
import * as mutations from './graphql/mutations';
import csvSync = require('csv-parse/lib/sync');
import * as fs from 'fs';
import GoogleSpreadsheet from "google-spreadsheet"

Amplify.configure(aws_config);

// 似たようなクエリインターフェースがたくさんできるのをなんとかしたい

async function listAllMakers() {
  var result = [];
  var nextToken;
  do {
    const makers = await API.graphql(graphqlOperation(queries.listMakers, {nextToken: nextToken}));
    nextToken = makers["data"]["listMakers"]["nextToken"];
    Array.prototype.push.apply(result, makers["data"]["listMakers"]["items"]);
  } while (nextToken);
  return result;
}

async function searchMakers(condition) {
  var result = [];
  var nextToken;
  do {
    const makers = await API.graphql(graphqlOperation(queries.listMakers, {filter: condition, nextToken: nextToken}));
    nextToken = makers["data"]["listMakers"]["nextToken"];
    Array.prototype.push.apply(result, makers["data"]["listMakers"]["items"]);
  } while (nextToken);
  return result;
}

async function updateOrCreateMaker(entity) {
  const searchResult = await searchMakers({name: {eq: entity["name"]}});
  // console.log('searched: ', JSON.stringify(searchResult));
  if (searchResult.length > 1) return;

  // なんとかしたい
  // console.log('entity: ', JSON.stringify(entity));
  if (searchResult.length == 0) {
    const params = {
      name: entity['name'],
      rank: entity['rank']
    };
    const newMaker = await API.graphql(graphqlOperation(mutations.createMaker, {input: params}));
    // console.log('create: ', JSON.stringify(newMaker));
  } else {
    const params = {
      id: searchResult[0]['id'],
      name: entity['name'],
      rank: entity['rank']
    };
    const newMaker = await API.graphql(graphqlOperation(mutations.updateMaker, {input: params}));
    // console.log('update: ',JSON.stringify(newMaker));
  }
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
      // console.log(maker);
      const oldMaker = await API.graphql(graphqlOperation(mutations.deleteMaker, {input: params}));
      // console.log(oldMaker);
    }
  } while (nextToken);
}

async function searchWaterServer(condition) {
  var result = [];
  var nextToken;
  do {
    const makers = await API.graphql(graphqlOperation(queries.listWaterServers, {filter: condition, nextToken: nextToken}));
    nextToken = makers["data"]["listWaterServers"]["nextToken"];
    Array.prototype.push.apply(result, makers["data"]["listWaterServers"]["items"]);
  } while (nextToken);
  return result;
}

async function updateOrCreateWaterServer(entity) {
  const searchResult = await searchWaterServer({name: {eq: entity["name"]}});
  // console.log('searched: ', JSON.stringify(searchResult));
  if (searchResult.length > 1) return;

  // console.log('entity: ', JSON.stringify(entity));
  if (searchResult.length == 0) {
    const newWaterServer = await API.graphql(graphqlOperation(mutations.createWaterServer, {input: entity}));
    // console.log('create: ', JSON.stringify(newWaterServer));
  } else {
    entity['id'] = searchResult[0]['id'];
    const newWaterServer = await API.graphql(graphqlOperation(mutations.updateWaterServer, {input: entity}));
    // console.log('update: ',JSON.stringify(newWaterServer));
  }
}

async function searchMakerDetails(condition) {
  var result = [];
  var nextToken;
  do {
    const makers = await API.graphql(graphqlOperation(queries.listMakerDetails, {filter: condition, nextToken: nextToken}));
    nextToken = makers["data"]["listMakerDetails"]["nextToken"];
    Array.prototype.push.apply(result, makers["data"]["listMakerDetails"]["items"]);
  } while (nextToken);
  return result;
}

async function updateOrCreateMakerDetails(entity) {
  const searchResult = await searchMakerDetails({maker_id: {eq: entity["maker_id"]}});
  console.log('searched: ', JSON.stringify(searchResult));
  if (searchResult.length > 1) return;

  console.log('entity: ', JSON.stringify(entity));
  if (searchResult.length == 0) {
    console.log('here');
    const details = await API.graphql(graphqlOperation(mutations.createMakerDetails, {input: entity}));
    console.log('create: ', JSON.stringify(details));
    await API.graphql(graphqlOperation(mutations.updateMaker, {input: {id: entity["maker_id"], maker_details_id: details["data"]["createMakerDetails"]["id"]}}))
  } else {
    entity['id'] = searchResult[0]['id'];
    const details = await API.graphql(graphqlOperation(mutations.updateMakerDetails, {input: entity}));
    console.log('update: ',JSON.stringify(details));
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
              width: rows[j]["幅"],
              depth: rows[j]["奥行"],
              height: rows[j]["高さ"]
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
      for (var j in rows) {
        if (!rows[j]["メーカー名"]) continue;
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
            delivery_cycle: rows[j]["周期"]
          })
        )
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

  const makers = await listAllMakers();
  var maker_id_by_name = {};
  for (var i in makers) {
    console.log(makers[i]);
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

          if (data.worksheets[i].title === "1. サーバー情報") {
            promises.push(updateWaterServersFromSheet(maker_id_by_name, data.worksheets[i]));
          }

          if (data.worksheets[i].title === "ランキング生成") {
            promises.push(updateMakerDetailsFromSheet(maker_id_by_name, data.worksheets[i]))
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
