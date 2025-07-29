# 🔍 COMPLETE DATA VALIDATION REPORT

## 📋 **EXECUTIVE SUMMARY**

This report validates the exact consistency between CSV data, S3 data, and Raw Database for 41 user-event pairs from Mixpanel campaign `ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign` (ID: `120223331225260178`) for July 16-29, 2025.

**GOAL**: Identify exactly where data loss occurs in the pipeline and confirm data integrity at each stage.

---

## 📊 **DATA SOURCE VALIDATION**

### **S3 DATA ANALYSIS**

#### **✅ USERS FOUND IN S3 (40/41)**
The following 40 users from the CSV were found in S3 user data:

1. `$device:D7284123-54E5-4BD0-91D1-58B769920351`
2. `nvGOajaWruW`
3. `$device:34286A65-A2D0-47C7-B813-D7D2B484375A`
4. `$device:25070c68-d026-4493-8494-d90defe462c9`
5. `q_qqe8ERkCN`
6. `$device:dacce5b8-eabf-4e63-ada9-bd3766054753`
7. `Y-XjO91Bmgq`
8. `F16D50BB-BC0F-4F64-A024-B429F9FC268B`
9. `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691`
10. `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97`
11. `ra66ordJYSg`
12. `TKkeCcrXCWZ`
13. `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0`
14. `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb`
15. `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B`
16. `PCsj51NLznN`
17. `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c`
18. `bOEIiAB8gXM`
19. `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82`
20. `$device:7204BF37-A513-4FA9-AA05-EBEB02324622`
21. `4BRqPWzmk-s`
22. `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA`
23. `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e`
24. `iSZOyNtsMJJ`
25. `aqgvtnXD57h`
26. `$device:5A08A236-EBB3-436F-B038-942A0E164EAD`
27. `qpJxwHfyAqh`
28. `wt2fQAUPWRj`
29. `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78`
30. `pe60vc5po2b`
31. `kTmZGnx23A2`
32. `$device:10AD4D44-C108-43D1-BD16-B39F4A734053`
33. `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b`
34. `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7`
35. `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84`
36. `$device:86fd9366-66d9-4954-9c9c-315e84f95467`
37. `irW5-suIWfq`
38. `RCaAet-2Sk7`
39. `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68`
40. `WLil4BvMdI0`

#### **❌ USERS MISSING FROM S3 (1/41)**
1. `MEBr6rMoQm1`

---

#### **✅ EVENTS FOUND IN S3 (40/41)**
The following 40 events from the CSV were found in S3 event data:

1. `09858561-e3a2-4227-996c-2c534f90b69f` → `$device:D7284123-54E5-4BD0-91D1-58B769920351`
2. `0e2d1c40-e33f-4048-909f-a6edd5e1335f` → `nvGOajaWruW`
3. `100b325f-a7ca-4a9f-88c8-4f570e05598d` → `$device:34286A65-A2D0-47C7-B813-D7D2B484375A`
4. `10ed06c0-7c52-4658-b263-3400f588c28e` → `$device:25070c68-d026-4493-8494-d90defe462c9`
5. `156394ae-13c6-42cf-a693-06cd570fb861` → `q_qqe8ERkCN`
6. `179535dd-a664-4534-94f7-5c662e99b791` → `$device:dacce5b8-eabf-4e63-ada9-bd3766054753`
7. `2193a2e8-22dd-4c54-b851-23441485d60f` → `Y-XjO91Bmgq`
8. `2fc9786a-63a9-4c3c-aca2-67a73edd0d6d` → `F16D50BB-BC0F-4F64-A024-B429F9FC268B`
9. `37a66b22-6378-4abd-8d5a-cedb7ab36e44` → `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691`
10. `4740c635-7844-4eea-8e11-82ebd2155cd2` → `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97`
11. `47872694-9cd4-4c9f-bbb3-348c63644b0f` → `ra66ordJYSg`
12. `4902e459-a63e-4bf0-b167-8f1469f0dd7b` → `TKkeCcrXCWZ`
13. `50e13688-964b-4600-865e-5401d88fbee8` → `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0`
14. `534ce39d-8fbd-4586-8010-113e8d4898db` → `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb`
15. `58f13984-38d0-4373-b273-e2005c39ac97` → `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B`
16. `69f91c4d-6fa2-4c31-91ea-0e08de50d477` → `PCsj51NLznN`
17. `7bb56679-d740-4ab3-8826-eaa43bd52532` → `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c`
18. `87ee1689-5cea-4441-8ad2-89b4b22fe5d9` → `bOEIiAB8gXM`
19. `8f29d77f-ae2f-43d4-8585-bdfc4d5fd58d` → `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82`
20. `91bc6428-cbe9-4d67-b8c0-93571eb8109e` → `$device:7204BF37-A513-4FA9-AA05-EBEB02324622`
21. `9675a2df-b13c-430e-8f75-da5c3c89a747` → `4BRqPWzmk-s`
22. `99ac3416-0697-4354-9fe1-c56c65cfe24c` → `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA`
23. `a1ee4830-36b9-4363-a470-56ecb392e638` → `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e`
24. `a2fab157-7021-4836-98be-ca18715c055e` → `iSZOyNtsMJJ`
25. `a62a14dc-1473-48fc-b5be-494b0e4e677f` → `aqgvtnXD57h`
26. `a9f0890d-1338-42d8-96c9-cfdedb7be9fa` → `$device:5A08A236-EBB3-436F-B038-942A0E164EAD`
27. `adbe1965-69e6-4418-8ddb-ffa4555685c1` → `qpJxwHfyAqh`
28. `b68acf65-367d-42bb-b5e0-36998c2eafa0` → `wt2fQAUPWRj`
29. `b7d4704a-f87b-4383-a2ef-c03c083e9bfb` → `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78`
30. `be2f97e4-d6b4-44c5-97bd-756545940490` → `pe60vc5po2b`
31. `c22939b5-210a-46d5-b9d7-5d0e0c045cdc` → `kTmZGnx23A2`
32. `c43181ac-63a3-4e7a-af0b-ba19aeec105a` → `$device:10AD4D44-C108-43D1-BD16-B39F4A734053`
33. `c51b7896-6686-4e4c-a1a5-d4a43f0e136f` → `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b`
34. `c76b025d-3625-4f24-83d1-f87eef0ee5f6` → `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7`
35. `cb928487-164e-49ad-a0e2-d3b6dbdc24de` → `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84`
36. `cde13cba-a7c4-4cb3-9a3f-7833047aec20` → `$device:86fd9366-66d9-4954-9c9c-315e84f95467`
37. `d3a17697-fd9a-4df7-87d2-e61b217681f0` → `irW5-suIWfq`
38. `d5786f6f-8224-4994-92c1-cd532108bc66` → `RCaAet-2Sk7`
39. `e4bdebce-84a6-4230-9e0a-0c804f0f29da` → `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68`
40. `e6b2f1a5-3f8a-41e1-a8c7-325ca7bef571` → `WLil4BvMdI0`

#### **❌ EVENTS MISSING FROM S3 (1/41)**
1. `75f87ac2-b124-4005-aade-5f02cb4babbc` → `MEBr6rMoQm1`

---

#### **✅ COMPLETE PAIRS (BOTH USER AND EVENT IN S3) - 40/41**
All 40 of the found users have their corresponding events, and all 40 found events have their corresponding users:

1. `$device:D7284123-54E5-4BD0-91D1-58B769920351` ↔ `09858561-e3a2-4227-996c-2c534f90b69f`
2. `nvGOajaWruW` ↔ `0e2d1c40-e33f-4048-909f-a6edd5e1335f`
3. `$device:34286A65-A2D0-47C7-B813-D7D2B484375A` ↔ `100b325f-a7ca-4a9f-88c8-4f570e05598d`
4. `$device:25070c68-d026-4493-8494-d90defe462c9` ↔ `10ed06c0-7c52-4658-b263-3400f588c28e`
5. `q_qqe8ERkCN` ↔ `156394ae-13c6-42cf-a693-06cd570fb861`
6. `$device:dacce5b8-eabf-4e63-ada9-bd3766054753` ↔ `179535dd-a664-4534-94f7-5c662e99b791`
7. `Y-XjO91Bmgq` ↔ `2193a2e8-22dd-4c54-b851-23441485d60f`
8. `F16D50BB-BC0F-4F64-A024-B429F9FC268B` ↔ `2fc9786a-63a9-4c3c-aca2-67a73edd0d6d`
9. `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691` ↔ `37a66b22-6378-4abd-8d5a-cedb7ab36e44`
10. `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97` ↔ `4740c635-7844-4eea-8e11-82ebd2155cd2`
11. `ra66ordJYSg` ↔ `47872694-9cd4-4c9f-bbb3-348c63644b0f`
12. `TKkeCcrXCWZ` ↔ `4902e459-a63e-4bf0-b167-8f1469f0dd7b`
13. `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0` ↔ `50e13688-964b-4600-865e-5401d88fbee8`
14. `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb` ↔ `534ce39d-8fbd-4586-8010-113e8d4898db`
15. `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B` ↔ `58f13984-38d0-4373-b273-e2005c39ac97`
16. `PCsj51NLznN` ↔ `69f91c4d-6fa2-4c31-91ea-0e08de50d477`
17. `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c` ↔ `7bb56679-d740-4ab3-8826-eaa43bd52532`
18. `bOEIiAB8gXM` ↔ `87ee1689-5cea-4441-8ad2-89b4b22fe5d9`
19. `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82` ↔ `8f29d77f-ae2f-43d4-8585-bdfc4d5fd58d`
20. `$device:7204BF37-A513-4FA9-AA05-EBEB02324622` ↔ `91bc6428-cbe9-4d67-b8c0-93571eb8109e`
21. `4BRqPWzmk-s` ↔ `9675a2df-b13c-430e-8f75-da5c3c89a747`
22. `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA` ↔ `99ac3416-0697-4354-9fe1-c56c65cfe24c`
23. `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e` ↔ `a1ee4830-36b9-4363-a470-56ecb392e638`
24. `iSZOyNtsMJJ` ↔ `a2fab157-7021-4836-98be-ca18715c055e`
25. `aqgvtnXD57h` ↔ `a62a14dc-1473-48fc-b5be-494b0e4e677f`
26. `$device:5A08A236-EBB3-436F-B038-942A0E164EAD` ↔ `a9f0890d-1338-42d8-96c9-cfdedb7be9fa`
27. `qpJxwHfyAqh` ↔ `adbe1965-69e6-4418-8ddb-ffa4555685c1`
28. `wt2fQAUPWRj` ↔ `b68acf65-367d-42bb-b5e0-36998c2eafa0`
29. `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78` ↔ `b7d4704a-f87b-4383-a2ef-c03c083e9bfb`
30. `pe60vc5po2b` ↔ `be2f97e4-d6b4-44c5-97bd-756545940490`
31. `kTmZGnx23A2` ↔ `c22939b5-210a-46d5-b9d7-5d0e0c045cdc`
32. `$device:10AD4D44-C108-43D1-BD16-B39F4A734053` ↔ `c43181ac-63a3-4e7a-af0b-ba19aeec105a`
33. `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b` ↔ `c51b7896-6686-4e4c-a1a5-d4a43f0e136f`
34. `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7` ↔ `c76b025d-3625-4f24-83d1-f87eef0ee5f6`
35. `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84` ↔ `cb928487-164e-49ad-a0e2-d3b6dbdc24de`
36. `$device:86fd9366-66d9-4954-9c9c-315e84f95467` ↔ `cde13cba-a7c4-4cb3-9a3f-7833047aec20`
37. `irW5-suIWfq` ↔ `d3a17697-fd9a-4df7-87d2-e61b217681f0`
38. `RCaAet-2Sk7` ↔ `d5786f6f-8224-4994-92c1-cd532108bc66`
39. `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68` ↔ `e4bdebce-84a6-4230-9e0a-0c804f0f29da`
40. `WLil4BvMdI0` ↔ `e6b2f1a5-3f8a-41e1-a8c7-325ca7bef571`

#### **❌ INCOMPLETE PAIR (MISSING BOTH USER AND EVENT) - 1/41**
1. `MEBr6rMoQm1` ↔ `75f87ac2-b124-4005-aade-5f02cb4babbc` *(Both user and event missing from S3)*

---

## 🔄 **RAW DATABASE VALIDATION**

### **USERS IN RAW DATABASE (41/41)**
The following 41 users from the CSV were found in Raw Database:

1. `$device:D7284123-54E5-4BD0-91D1-58B769920351`
2. `nvGOajaWruW`
3. `$device:34286A65-A2D0-47C7-B813-D7D2B484375A`
4. `$device:25070c68-d026-4493-8494-d90defe462c9`
5. `q_qqe8ERkCN`
6. `$device:dacce5b8-eabf-4e63-ada9-bd3766054753`
7. `Y-XjO91Bmgq`
8. `F16D50BB-BC0F-4F64-A024-B429F9FC268B`
9. `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691`
10. `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97`
11. `ra66ordJYSg`
12. `TKkeCcrXCWZ`
13. `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0`
14. `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb`
15. `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B`
16. `PCsj51NLznN`
17. `MEBr6rMoQm1`
18. `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c`
19. `bOEIiAB8gXM`
20. `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82`
21. `$device:7204BF37-A513-4FA9-AA05-EBEB02324622`
22. `4BRqPWzmk-s`
23. `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA`
24. `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e`
25. `iSZOyNtsMJJ`
26. `aqgvtnXD57h`
27. `$device:5A08A236-EBB3-436F-B038-942A0E164EAD`
28. `qpJxwHfyAqh`
29. `wt2fQAUPWRj`
30. `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78`
31. `pe60vc5po2b`
32. `kTmZGnx23A2`
33. `$device:10AD4D44-C108-43D1-BD16-B39F4A734053`
34. `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b`
35. `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7`
36. `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84`
37. `$device:86fd9366-66d9-4954-9c9c-315e84f95467`
38. `irW5-suIWfq`
39. `RCaAet-2Sk7`
40. `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68`
41. `WLil4BvMdI0`


### **EVENTS IN RAW DATABASE (41/41)**
The following 41 events from the CSV were found in Raw Database:

1. `09858561-e3a2-4227-996c-2c534f90b69f` → `$device:D7284123-54E5-4BD0-91D1-58B769920351`
2. `0e2d1c40-e33f-4048-909f-a6edd5e1335f` → `nvGOajaWruW`
3. `100b325f-a7ca-4a9f-88c8-4f570e05598d` → `$device:34286A65-A2D0-47C7-B813-D7D2B484375A`
4. `10ed06c0-7c52-4658-b263-3400f588c28e` → `$device:25070c68-d026-4493-8494-d90defe462c9`
5. `156394ae-13c6-42cf-a693-06cd570fb861` → `q_qqe8ERkCN`
6. `179535dd-a664-4534-94f7-5c662e99b791` → `$device:dacce5b8-eabf-4e63-ada9-bd3766054753`
7. `2193a2e8-22dd-4c54-b851-23441485d60f` → `Y-XjO91Bmgq`
8. `2fc9786a-63a9-4c3c-aca2-67a73edd0d6d` → `F16D50BB-BC0F-4F64-A024-B429F9FC268B`
9. `37a66b22-6378-4abd-8d5a-cedb7ab36e44` → `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691`
10. `4740c635-7844-4eea-8e11-82ebd2155cd2` → `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97`
11. `47872694-9cd4-4c9f-bbb3-348c63644b0f` → `ra66ordJYSg`
12. `4902e459-a63e-4bf0-b167-8f1469f0dd7b` → `TKkeCcrXCWZ`
13. `50e13688-964b-4600-865e-5401d88fbee8` → `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0`
14. `534ce39d-8fbd-4586-8010-113e8d4898db` → `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb`
15. `58f13984-38d0-4373-b273-e2005c39ac97` → `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B`
16. `69f91c4d-6fa2-4c31-91ea-0e08de50d477` → `PCsj51NLznN`
17. `75f87ac2-b124-4005-aade-5f02cb4babbc` → `MEBr6rMoQm1`
18. `7bb56679-d740-4ab3-8826-eaa43bd52532` → `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c`
19. `87ee1689-5cea-4441-8ad2-89b4b22fe5d9` → `bOEIiAB8gXM`
20. `8f29d77f-ae2f-43d4-8585-bdfc4d5fd58d` → `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82`
21. `91bc6428-cbe9-4d67-b8c0-93571eb8109e` → `$device:7204BF37-A513-4FA9-AA05-EBEB02324622`
22. `9675a2df-b13c-430e-8f75-da5c3c89a747` → `4BRqPWzmk-s`
23. `99ac3416-0697-4354-9fe1-c56c65cfe24c` → `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA`
24. `a1ee4830-36b9-4363-a470-56ecb392e638` → `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e`
25. `a2fab157-7021-4836-98be-ca18715c055e` → `iSZOyNtsMJJ`
26. `a62a14dc-1473-48fc-b5be-494b0e4e677f` → `aqgvtnXD57h`
27. `a9f0890d-1338-42d8-96c9-cfdedb7be9fa` → `$device:5A08A236-EBB3-436F-B038-942A0E164EAD`
28. `adbe1965-69e6-4418-8ddb-ffa4555685c1` → `qpJxwHfyAqh`
29. `b68acf65-367d-42bb-b5e0-36998c2eafa0` → `wt2fQAUPWRj`
30. `b7d4704a-f87b-4383-a2ef-c03c083e9bfb` → `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78`
31. `be2f97e4-d6b4-44c5-97bd-756545940490` → `pe60vc5po2b`
32. `c22939b5-210a-46d5-b9d7-5d0e0c045cdc` → `kTmZGnx23A2`
33. `c43181ac-63a3-4e7a-af0b-ba19aeec105a` → `$device:10AD4D44-C108-43D1-BD16-B39F4A734053`
34. `c51b7896-6686-4e4c-a1a5-d4a43f0e136f` → `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b`
35. `c76b025d-3625-4f24-83d1-f87eef0ee5f6` → `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7`
36. `cb928487-164e-49ad-a0e2-d3b6dbdc24de` → `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84`
37. `cde13cba-a7c4-4cb3-9a3f-7833047aec20` → `$device:86fd9366-66d9-4954-9c9c-315e84f95467`
38. `d3a17697-fd9a-4df7-87d2-e61b217681f0` → `irW5-suIWfq`
39. `d5786f6f-8224-4994-92c1-cd532108bc66` → `RCaAet-2Sk7`
40. `e4bdebce-84a6-4230-9e0a-0c804f0f29da` → `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68`
41. `e6b2f1a5-3f8a-41e1-a8c7-325ca7bef571` → `WLil4BvMdI0`


### **COMPLETE PAIRS IN RAW DATABASE (41/41)**
The following 41 complete pairs (both user AND event) were found in Raw Database:

1. `$device:D7284123-54E5-4BD0-91D1-58B769920351` ↔ `09858561-e3a2-4227-996c-2c534f90b69f`
2. `nvGOajaWruW` ↔ `0e2d1c40-e33f-4048-909f-a6edd5e1335f`
3. `$device:34286A65-A2D0-47C7-B813-D7D2B484375A` ↔ `100b325f-a7ca-4a9f-88c8-4f570e05598d`
4. `$device:25070c68-d026-4493-8494-d90defe462c9` ↔ `10ed06c0-7c52-4658-b263-3400f588c28e`
5. `q_qqe8ERkCN` ↔ `156394ae-13c6-42cf-a693-06cd570fb861`
6. `$device:dacce5b8-eabf-4e63-ada9-bd3766054753` ↔ `179535dd-a664-4534-94f7-5c662e99b791`
7. `Y-XjO91Bmgq` ↔ `2193a2e8-22dd-4c54-b851-23441485d60f`
8. `F16D50BB-BC0F-4F64-A024-B429F9FC268B` ↔ `2fc9786a-63a9-4c3c-aca2-67a73edd0d6d`
9. `$device:B1BB1A8F-B45A-4BD8-8B0D-1CEB183EE691` ↔ `37a66b22-6378-4abd-8d5a-cedb7ab36e44`
10. `$device:D1E12E7A-0BD9-4FDC-89AC-6241EC409B97` ↔ `4740c635-7844-4eea-8e11-82ebd2155cd2`
11. `ra66ordJYSg` ↔ `47872694-9cd4-4c9f-bbb3-348c63644b0f`
12. `TKkeCcrXCWZ` ↔ `4902e459-a63e-4bf0-b167-8f1469f0dd7b`
13. `197759f7b9424a0-0de06ee95ceb098-4b5c4769-51bf4-197759f7b9531b0` ↔ `50e13688-964b-4600-865e-5401d88fbee8`
14. `$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb` ↔ `534ce39d-8fbd-4586-8010-113e8d4898db`
15. `$device:A7F8B109-EAC4-4A3B-97E5-0325DDB2453B` ↔ `58f13984-38d0-4373-b273-e2005c39ac97`
16. `PCsj51NLznN` ↔ `69f91c4d-6fa2-4c31-91ea-0e08de50d477`
17. `MEBr6rMoQm1` ↔ `75f87ac2-b124-4005-aade-5f02cb4babbc`
18. `$device:e0d83f09-76ad-4095-945e-94ac170ffd5c` ↔ `7bb56679-d740-4ab3-8826-eaa43bd52532`
19. `bOEIiAB8gXM` ↔ `87ee1689-5cea-4441-8ad2-89b4b22fe5d9`
20. `196618f0ae31e29-02371046ec0cfc-254f3316-61d78-196618f0ae43c82` ↔ `8f29d77f-ae2f-43d4-8585-bdfc4d5fd58d`
21. `$device:7204BF37-A513-4FA9-AA05-EBEB02324622` ↔ `91bc6428-cbe9-4d67-b8c0-93571eb8109e`
22. `4BRqPWzmk-s` ↔ `9675a2df-b13c-430e-8f75-da5c3c89a747`
23. `$device:A265AB98-0C8B-44DE-9357-E340AED25EAA` ↔ `99ac3416-0697-4354-9fe1-c56c65cfe24c`
24. `190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e` ↔ `a1ee4830-36b9-4363-a470-56ecb392e638`
25. `iSZOyNtsMJJ` ↔ `a2fab157-7021-4836-98be-ca18715c055e`
26. `aqgvtnXD57h` ↔ `a62a14dc-1473-48fc-b5be-494b0e4e677f`
27. `$device:5A08A236-EBB3-436F-B038-942A0E164EAD` ↔ `a9f0890d-1338-42d8-96c9-cfdedb7be9fa`
28. `qpJxwHfyAqh` ↔ `adbe1965-69e6-4418-8ddb-ffa4555685c1`
29. `wt2fQAUPWRj` ↔ `b68acf65-367d-42bb-b5e0-36998c2eafa0`
30. `19507e8d70c5c1-00aa663c9e908a-59361b3d-36380-19507e8d70d78` ↔ `b7d4704a-f87b-4383-a2ef-c03c083e9bfb`
31. `pe60vc5po2b` ↔ `be2f97e4-d6b4-44c5-97bd-756545940490`
32. `kTmZGnx23A2` ↔ `c22939b5-210a-46d5-b9d7-5d0e0c045cdc`
33. `$device:10AD4D44-C108-43D1-BD16-B39F4A734053` ↔ `c43181ac-63a3-4e7a-af0b-ba19aeec105a`
34. `196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b` ↔ `c51b7896-6686-4e4c-a1a5-d4a43f0e136f`
35. `196ecce56ac636-03a8bb5928135-406d2a10-505c8-196ecce56ad1fe7` ↔ `c76b025d-3625-4f24-83d1-f87eef0ee5f6`
36. `$device:58C8D79E-13E5-438F-901B-D6CFD7A22A84` ↔ `cb928487-164e-49ad-a0e2-d3b6dbdc24de`
37. `$device:86fd9366-66d9-4954-9c9c-315e84f95467` ↔ `cde13cba-a7c4-4cb3-9a3f-7833047aec20`
38. `irW5-suIWfq` ↔ `d3a17697-fd9a-4df7-87d2-e61b217681f0`
39. `RCaAet-2Sk7` ↔ `d5786f6f-8224-4994-92c1-cd532108bc66`
40. `1975cc58a03e9a-04d18aa413cd1a-525c2626-505c8-1975cc58a041b68` ↔ `e4bdebce-84a6-4230-9e0a-0c804f0f29da`
41. `WLil4BvMdI0` ↔ `e6b2f1a5-3f8a-41e1-a8c7-325ca7bef571`

---

## 📊 **S3 vs RAW DATABASE COMPARISON**

### **IDENTICAL DATA VERIFICATION**
- ❌ Users match between S3 and Raw DB (41/40 S3 users found in Raw DB)
- ❌ Events match between S3 and Raw DB (41/40 S3 events found in Raw DB)
- ❌ Complete pairs match between S3 and Raw DB (41/40 S3 pairs found in Raw DB)

### **DISCREPANCIES IDENTIFIED**
- **USER DISCREPANCY**: -1 users present in S3 but missing from Raw DB
- **EVENT DISCREPANCY**: -1 events present in S3 but missing from Raw DB
- **COMPLETE PAIRS DISCREPANCY**: -1 complete pairs present in S3 but missing from Raw DB

---

## 🎯 **CONCLUSIONS**

### **S3 DATA INTEGRITY**
- ✅ **Perfect 1:1 mapping confirmed** in S3 data
- ✅ **40/41 complete pairs available** for processing
- ❌ **1/41 missing** due to Mixpanel source data issue (`MEBr6rMoQm1`)

### **PIPELINE HEALTH INDICATORS**
- ❌ S3 → Raw DB user transfer integrity (**41/40 transferred**)
- ❌ S3 → Raw DB event transfer integrity (**41/40 transferred**)
- ❌ Data consistency verification
- ❌ Bug location identification: **S3 → Raw DB pipeline issue**

*Generated on: $(date)*
*Investigation: Mixpanel Trial Count Discrepancy*
*Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign (120223331225260178)* 