module.exports = {
  entry: './src/index.ts',
  target: 'node',
  cache: true,
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: ['ts-loader'],
        exclude: /node_modules/
      }
    ]
  },
  resolve: {
    extensions: [ '.tsx', '.ts', '.js' ]
  },
  mode: 'development'
};
