#TEST
SELECT GA4.defaultChannelGrouping("ab" ,"sdf",  "not" , [], [], [], []) output, "N/A for (source/medium/campaign) = (sdf/ab/not)" expected
UNION ALL
SELECT GA4.defaultChannelGrouping("" ,"",  "Cross-network" , [], [], [], []), "Cross-network"
UNION ALL
SELECT GA4.defaultChannelGrouping("(none)" ,"(direct)",  "" , [], [], [], []), "Direct"
UNION ALL
SELECT GA4.defaultChannelGrouping("cpc" ,"asdf",  "shop" , [], [], [], []), "Paid Shopping"
UNION ALL
SELECT GA4.defaultChannelGrouping("display" ,"asdf",  "a" , [], [], [], []), "Display"
UNION ALL
SELECT GA4.defaultChannelGrouping("my medium" ,"asdf",  "shopping" , [], [], [], []), "Organic Shopping"
UNION ALL
SELECT GA4.defaultChannelGrouping("referral" ,"asdf",  "fds" , [], [], [], []), "Referral"
UNION ALL
SELECT GA4.defaultChannelGrouping("sms" ,"asdf",  "fds" , [], [], [], []), "SMS"
UNION ALL
SELECT GA4.defaultChannelGrouping("cpc" ,"youtube.com",  "fds" , [], [], [], []), "Paid Video"
UNION ALL
SELECT GA4.defaultChannelGrouping("cpc" ,"myvideoplayer.com",  "fds" , [], [], [], ["myvideoplayer.com"]), "Paid Video"
