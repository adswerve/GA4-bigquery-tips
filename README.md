# GA4 Bigquery Queries And Tips
Helpful queries on top of the GA4 BigQuery export. Content here follows Adswerve's GA4 BigQuery tips blog posts.

## Blog Articles
1. [Universal Analytics to GA4 BigQuery Export Guide](https://adswerve.com/blog/universal-analytics-to-ga4-bigquery-export-guide/)
2. [GA4 BigQuery Export Guide - Users and Sessions](https://adswerve.com/blog/ga4-bigquery-guide-users-and-sessions-part-one/)
3. [GA4 BigQuery Export Guide - Event Parameters and Other Repeated Fields](https://adswerve.com/blog/ga4-bigquery-tips-event-parameters-and-other-repeated-fields-part-two/)
4. [GA4 BigQuery Export Guide - Guide to Attribution](https://adswerve.com/blog/ga4-bigquery-tips-guide-to-attribution/)
5. [Using Unpivot and Qualify with GA4 BQ Export](https://adswerve.com/blog/using-bigquerys-new-unpivot-and-qualify-features-with-the-ga4-export/)
6. [GA4 BigQuery Export - A Quick Overview](https://adswerve.com/blog/the-google-analytics-4-bigquery-export-a-quick-overview/)

## Queries in this Repository
### traffic-source-query.sql
This query tries to replicate session traffic source information from the GA4 UI. By identifying the first source of each session and also taking care of "direct" visits from users with previously identified traffic source.

### rule-based-attribution-modeling.sql
This query includes 5 different attribution models on top of the GA4 BigQuery export taking the traffic-source-query.sql as an input.

### channelGrouping.sql
A definition of a UDF, that takes medium, source, campaign and potential additional site categorizations to provide GA4-like channel grouping. The UDF was built based on the official documentation.

### channelGrouping_test.sql
A few quick tests of the channel grouping function.
