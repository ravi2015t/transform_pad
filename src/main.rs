// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions[<35;166;30M[<35;167;30M[<35;166;30M and limitations
// under the License.

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use tokio::time::Instant;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();
    let r = 10;

    let start = Instant::now();
    // register parquet file with the execution context

    ctx.register_parquet(
        "pad",
        &format!("pa_detail.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let query = (1..=48)
        .map(|i| format!("amount{}", i))
        .map(|column_name| format!("SUM({}) as {}", column_name, column_name))
        .collect::<Vec<String>>()
        .join(", ");

    let sql_query = format!("SELECT stop_date, {} FROM pad GROUP BY stop_date", query);
    // execute the query
    let df = ctx.sql(&sql_query).await?;

    let end = Instant::now();
    println!("Total time elapsed {:?}", end - start);

    let options = DataFrameWriteOptions::new();
    let opts = options.with_single_file_output(true);

    df.clone().write_json("./result", opts).await?;
    let end = Instant::now();
    println!("Total time elapsed {:?}", end - start);

    Ok(())
}
