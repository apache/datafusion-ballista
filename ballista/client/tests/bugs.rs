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
// specific language governing permissions and limitations
// under the License.

mod common;

// this tests require changes of RUST_MIN_STACK before run
//
// ```
// export RUST_MIN_STACK=20971520
// cargo test --features test_extended_stack --test bugs
// ```
#[cfg(test)]
#[cfg(feature = "standalone")]
#[cfg(feature = "test_extended_stack")]
mod extended {

    use ballista::prelude::SessionContextExt;
    use datafusion::{assert_batches_eq, prelude::*};
    //
    // tests bug: [Failed to execute TPC-DS Q47 in ballista](https://github.com/apache/datafusion-ballista/issues/1296)
    // where query execution fails due to missing sort information
    //
    #[tokio::test]
    async fn bug_1296_basic() -> datafusion::error::Result<()> {
        let test_data = crate::common::example_test_data();

        let ctx = SessionContext::standalone().await?;

        ctx.register_csv(
            "date_dim",
            &format!("{}/bug_1296/date_dim.csv", test_data),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        ctx.register_csv(
            "store",
            &format!("{}/bug_1296/store.csv", test_data),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        ctx.register_csv(
            "item",
            &format!("{}/bug_1296/item.csv", test_data),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        ctx.register_csv(
            "store_sales",
            &format!("{}/bug_1296/store_sales.csv", test_data),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        let query = r#"
with v1 as(
    select i_category,
           i_brand,
           s_store_name,
           s_company_name,
           d_year,
           d_moy,
           sum(ss_sales_price) sum_sales,
           avg(sum(ss_sales_price)) over
        (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
            rank() over
        (partition by i_category, i_brand,
                   s_store_name, s_company_name
         order by d_year, d_moy) rn
    from item, store_sales, date_dim, store
    where ss_item_sk = i_item_sk and
            ss_sold_date_sk = d_date_sk
      and ss_store_sk = s_store_sk and
        (
                    d_year = 1999 or
                    ( d_year = 1999-1 and d_moy =12) or
                    ( d_year = 1999+1 and d_moy =1)
            )
    group by i_category, i_brand,
             s_store_name, s_company_name,
             d_year, d_moy),
     v2 as(
         select v1.i_category,
                v1.i_brand,
                v1.s_store_name,
                v1.s_company_name,
                v1.d_year,
                v1.d_moy,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales psum,
                v1_lead.sum_sales nsum
         from v1, v1 v1_lag, v1 v1_lead
         where v1.i_category = v1_lag.i_category and
                 v1.i_category = v1_lead.i_category and
                 v1.i_brand = v1_lag.i_brand and
                 v1.i_brand = v1_lead.i_brand and
                 v1.s_store_name = v1_lag.s_store_name and
                 v1.s_store_name = v1_lead.s_store_name and
                 v1.s_company_name = v1_lag.s_company_name and
                 v1.s_company_name = v1_lead.s_company_name and
                 v1.rn = v1_lag.rn + 1 and
                 v1.rn = v1_lead.rn - 1)
select  *
from v2
where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
    limit 100;
    "#;

        let result = ctx.sql(query).await?.collect().await?;
        let expected = [
            "+-------------+-----------+----------------+----------------+--------+-------+--------------------+-----------+---------+---------+",
            "| i_category  | i_brand   | s_store_name   | s_company_name | d_year | d_moy | avg_monthly_sales  | sum_sales | psum    | nsum    |",
            "+-------------+-----------+----------------+----------------+--------+-------+--------------------+-----------+---------+---------+",
            "| Electronics | TechBrand | Downtown Store | Retail Corp    | 1999   | 4     | 1499.9850000000001 | 999.99    | 999.99  | 999.99  |",
            "| Electronics | TechBrand | Downtown Store | Retail Corp    | 1999   | 3     | 1499.9850000000001 | 999.99    | 1999.98 | 999.99  |",
            "| Electronics | TechBrand | Downtown Store | Retail Corp    | 1999   | 1     | 1499.9850000000001 | 1999.98   | 1999.98 | 1999.98 |",
            "| Electronics | TechBrand | Downtown Store | Retail Corp    | 1999   | 2     | 1499.9850000000001 | 1999.98   | 1999.98 | 999.99  |",
            "+-------------+-----------+----------------+----------------+--------+-------+--------------------+-----------+---------+---------+",
        ];

        assert_batches_eq!(expected, &result);

        Ok(())
    }
}
