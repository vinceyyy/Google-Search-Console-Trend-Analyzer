# Trend Analyzer
An automation tool used for analyze Google Search trend assiciate with the website.

## Current Feature
* Retrive data of given date range from Google Search Console API
* Identify similar keywords using fuzzy match
* Filter keywords with top performance (clicks, impressions) and calculate slope using linear regression
* Output 1: Valuable keywords list (top performance and uptrend), for strategic planning
* Output 2: Valuable queries with Google Search positions, for SEO improvement

## Future Roadmap
* Add cross-reference with Google Trends.
* Replace simple iteration with better implementation of fuzzywuzzy to improve performance.
* Replace manual categorization with auto categorization, using function instead of fixed number.
* Replace simple linear regression with better regression method, potentially using one that can predict timeseries data.
* Replace fuzzywuzzy with better categorization strategy.