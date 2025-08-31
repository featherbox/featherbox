export const testConnections = {
  sqlite: {
    name: 'test_sqlite_connection',
    type: 'sqlite',
    config: {
      path: './test.db',
    },
  },
  mysql: {
    name: 'test_mysql_connection',
    type: 'mysql',
    config: {
      host: 'localhost',
      port: '3306',
      database: 'test_db',
      username: 'test_user',
      password: 'test_pass',
    },
  },
  postgresql: {
    name: 'test_postgres_connection',
    type: 'postgresql',
    config: {
      host: 'localhost',
      port: '5432',
      database: 'test_db',
      username: 'test_user',
      password: 'test_pass',
    },
  },
};

export const testAdapters = {
  csv: {
    name: 'test_csv_adapter',
    type: 'csv',
    config: {
      path: './data/test.csv',
      delimiter: ',',
      header: 'true',
    },
  },
  json: {
    name: 'test_json_adapter',
    type: 'json',
    config: {
      path: './data/test.json',
    },
  },
  parquet: {
    name: 'test_parquet_adapter',
    type: 'parquet',
    config: {
      path: './data/test.parquet',
    },
  },
};

export const testModels = {
  simple: {
    name: 'test_simple_model',
    path: 'test/simple',
    sql: 'SELECT 1 as test_column',
  },
  aggregation: {
    name: 'test_aggregation_model',
    path: 'test/aggregation',
    sql: 'SELECT COUNT(*) as total FROM {{ ref("test_simple_model") }}',
  },
  transformation: {
    name: 'test_transformation_model',
    path: 'test/transformation',
    sql: `
      SELECT 
        test_column * 2 as doubled_value,
        'transformed' as status
      FROM {{ ref("test_simple_model") }}
    `,
  },
};
