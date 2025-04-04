if __name__ == '__main__':
    import pandas as pd
    from pandas import Timestamp

    from mecon.data import graphs

    df_income = pd.DataFrame({'amount': [216.85280044918753, 321.70356861613186, 376.785723960502, 422.55312156359713, 483.0836736839134, 53.32526745000238, 225.01760313858153, 458.3435694521627, 1838.8923386123383, 2424.7280865602, 1516.256001398668, 1699.5290587329446, 2727.377848747178, 2511.1519276756753, 899.7250891698823, 2049.766948749176, 527.6450836789314, 693.3640987886547, 1802.4875856254346, 1964.0708036441295, 1913.7278484663993, 390.5359794008985, 560.801523611256, 2181.1090131053033, 2212.961819491469, 1898.28132274471, 650.2192600416529, 3578.229701983173, 302.5848167147579, 336.7165137735728, 1872.0420842505027, 2931.1544967023297, 509.2552372445905, 2635.23352131447, 2509.2014597639336, 1043.6196458256782, 2028.7241583500463, 2863.5116445334124, 2248.335682224395, 422.0262896301018, 1851.6021168791701, 515.6707658087043, 866.7133845547554, 2851.105432424705, 283.1766975398905, 3980.6500631413796, 2733.4553039101347, 716.7335158097763, 661.2713116251113, 1722.961135459805, 174.58778683557574, 26.789627346743615, 304.94350460523054, 422.4954606471343, 237.96208578793093, 173.1920378411439, 339.5824276134464, 2495.597370812365, 2776.3525276889754, 1121.19882923138, 1582.5256957606998, 3264.646355265179, 2512.879534257036, 329.7747604159268, 471.8095878162782, 2095.9778885918663, 2262.3744285016332, 3656.426383658652, 530.2410163215602, 48.8160261265701],
                              'datetime': [Timestamp('2019-05-01 00:00:00'), Timestamp('2019-06-01 00:00:00'),
                                           Timestamp('2019-07-01 00:00:00'), Timestamp('2019-08-01 00:00:00'),
                                           Timestamp('2019-09-01 00:00:00'), Timestamp('2019-10-01 00:00:00'),
                                           Timestamp('2019-11-01 00:00:00'), Timestamp('2019-12-01 00:00:00'),
                                           Timestamp('2020-01-01 00:00:00'), Timestamp('2020-02-01 00:00:00'),
                                           Timestamp('2020-03-01 00:00:00'), Timestamp('2020-04-01 00:00:00'),
                                           Timestamp('2020-05-01 00:00:00'), Timestamp('2020-06-01 00:00:00'),
                                           Timestamp('2020-07-01 00:00:00'), Timestamp('2020-08-01 00:00:00'),
                                           Timestamp('2020-09-01 00:00:00'), Timestamp('2020-10-01 00:00:00'),
                                           Timestamp('2020-11-01 00:00:00'), Timestamp('2020-12-01 00:00:00'),
                                           Timestamp('2021-01-01 00:00:00'), Timestamp('2021-02-01 00:00:00'),
                                           Timestamp('2021-03-01 00:00:00'), Timestamp('2021-04-01 00:00:00'),
                                           Timestamp('2021-05-01 00:00:00'), Timestamp('2021-06-01 00:00:00'),
                                           Timestamp('2021-07-01 00:00:00'), Timestamp('2021-08-01 00:00:00'),
                                           Timestamp('2021-09-01 00:00:00'), Timestamp('2021-10-01 00:00:00'),
                                           Timestamp('2021-11-01 00:00:00'), Timestamp('2021-12-01 00:00:00'),
                                           Timestamp('2022-01-01 00:00:00'), Timestamp('2022-02-01 00:00:00'),
                                           Timestamp('2022-03-01 00:00:00'), Timestamp('2022-04-01 00:00:00'),
                                           Timestamp('2022-05-01 00:00:00'), Timestamp('2022-06-01 00:00:00'),
                                           Timestamp('2022-07-01 00:00:00'), Timestamp('2022-08-01 00:00:00'),
                                           Timestamp('2022-09-01 00:00:00'), Timestamp('2022-10-01 00:00:00'),
                                           Timestamp('2022-11-01 00:00:00'), Timestamp('2022-12-01 00:00:00'),
                                           Timestamp('2023-01-01 00:00:00'), Timestamp('2023-02-01 00:00:00'),
                                           Timestamp('2023-03-01 00:00:00'), Timestamp('2023-04-01 00:00:00'),
                                           Timestamp('2023-05-01 00:00:00'), Timestamp('2023-06-01 00:00:00'),
                                           Timestamp('2023-07-01 00:00:00'), Timestamp('2023-08-01 00:00:00'),
                                           Timestamp('2023-09-01 00:00:00'), Timestamp('2023-10-01 00:00:00'),
                                           Timestamp('2023-11-01 00:00:00'), Timestamp('2023-12-01 00:00:00'),
                                           Timestamp('2024-01-01 00:00:00'), Timestamp('2024-02-01 00:00:00'),
                                           Timestamp('2024-03-01 00:00:00'), Timestamp('2024-04-01 00:00:00'),
                                           Timestamp('2024-05-01 00:00:00'), Timestamp('2024-06-01 00:00:00'),
                                           Timestamp('2024-07-01 00:00:00'), Timestamp('2024-08-01 00:00:00'),
                                           Timestamp('2024-09-01 00:00:00'), Timestamp('2024-10-01 00:00:00'),
                                           Timestamp('2024-11-01 00:00:00'), Timestamp('2024-12-01 00:00:00'),
                                           Timestamp('2025-01-01 00:00:00'), Timestamp('2025-02-01 00:00:00')],
                              })
    df_rent = pd.DataFrame({'amount': [425.84410868412556, 437.74223470951534, 152.40491171163396, 54.16287160859168, 414.5326021284253, 132.7118821751258, 113.9294466932242, 59.505864321506785, 1724.100812450046, 1533.0762980299905, 2898.954733953963, 737.7516333024902, 1257.1873877110029, 1779.5273685832312, 777.9543785700891, 2901.625939876436, 1247.264404525632, 1020.6347783274014, 556.9411293606734, 1453.0017430359535, 1975.162717645735, 908.1356510698919, 1849.4419354320694, 1083.499045250736, 1824.6947116171123, 1831.511467487534, 292.19857621492554, 3847.858626832257, 260.56497795028974, 2119.010259071224, 2337.11288356617, 4150.94911311865, 505.78289072511683, 2442.3314949234973, 1211.4667263078686, 2801.315456029697, 3027.1870067172176, 401.3654580773252, 546.9154027003541, 3153.020875607475, 1490.733664002537, 2173.5370750648244, 1850.2790245380172, 2063.0315350237624, 1540.5103784019593, 648.0620416604704, 1752.2181540107629, 2050.4366271256126, 594.7568851565455, 2644.311444815379, 72.1915425221381, 169.56604010293873, 336.3232179233852, 124.46566533940256, 338.80026155282815, 114.76947788193581, 161.39467025066423, 2315.553223746136, 4744.9367195127315, 3423.882076057784, 3818.8774116183768, 696.5470434349753, 1379.382474316903, 2044.1011849475, 1904.094281424594, 2492.6215060327713, 1085.012635549952, 3020.664483654791, 4943.696347862488, 448.9501592817871],
                            'datetime': [Timestamp('2019-05-01 00:00:00'), Timestamp('2019-06-01 00:00:00'),
                                         Timestamp('2019-07-01 00:00:00'), Timestamp('2019-08-01 00:00:00'),
                                         Timestamp('2019-09-01 00:00:00'), Timestamp('2019-10-01 00:00:00'),
                                         Timestamp('2019-11-01 00:00:00'), Timestamp('2019-12-01 00:00:00'),
                                         Timestamp('2020-01-01 00:00:00'), Timestamp('2020-02-01 00:00:00'),
                                         Timestamp('2020-03-01 00:00:00'), Timestamp('2020-04-01 00:00:00'),
                                         Timestamp('2020-05-01 00:00:00'), Timestamp('2020-06-01 00:00:00'),
                                         Timestamp('2020-07-01 00:00:00'), Timestamp('2020-08-01 00:00:00'),
                                         Timestamp('2020-09-01 00:00:00'), Timestamp('2020-10-01 00:00:00'),
                                         Timestamp('2020-11-01 00:00:00'), Timestamp('2020-12-01 00:00:00'),
                                         Timestamp('2021-01-01 00:00:00'), Timestamp('2021-02-01 00:00:00'),
                                         Timestamp('2021-03-01 00:00:00'), Timestamp('2021-04-01 00:00:00'),
                                         Timestamp('2021-05-01 00:00:00'), Timestamp('2021-06-01 00:00:00'),
                                         Timestamp('2021-07-01 00:00:00'), Timestamp('2021-08-01 00:00:00'),
                                         Timestamp('2021-09-01 00:00:00'), Timestamp('2021-10-01 00:00:00'),
                                         Timestamp('2021-11-01 00:00:00'), Timestamp('2021-12-01 00:00:00'),
                                         Timestamp('2022-01-01 00:00:00'), Timestamp('2022-02-01 00:00:00'),
                                         Timestamp('2022-03-01 00:00:00'), Timestamp('2022-04-01 00:00:00'),
                                         Timestamp('2022-05-01 00:00:00'), Timestamp('2022-06-01 00:00:00'),
                                         Timestamp('2022-07-01 00:00:00'), Timestamp('2022-08-01 00:00:00'),
                                         Timestamp('2022-09-01 00:00:00'), Timestamp('2022-10-01 00:00:00'),
                                         Timestamp('2022-11-01 00:00:00'), Timestamp('2022-12-01 00:00:00'),
                                         Timestamp('2023-01-01 00:00:00'), Timestamp('2023-02-01 00:00:00'),
                                         Timestamp('2023-03-01 00:00:00'), Timestamp('2023-04-01 00:00:00'),
                                         Timestamp('2023-05-01 00:00:00'), Timestamp('2023-06-01 00:00:00'),
                                         Timestamp('2023-07-01 00:00:00'), Timestamp('2023-08-01 00:00:00'),
                                         Timestamp('2023-09-01 00:00:00'), Timestamp('2023-10-01 00:00:00'),
                                         Timestamp('2023-11-01 00:00:00'), Timestamp('2023-12-01 00:00:00'),
                                         Timestamp('2024-01-01 00:00:00'), Timestamp('2024-02-01 00:00:00'),
                                         Timestamp('2024-03-01 00:00:00'), Timestamp('2024-04-01 00:00:00'),
                                         Timestamp('2024-05-01 00:00:00'), Timestamp('2024-06-01 00:00:00'),
                                         Timestamp('2024-07-01 00:00:00'), Timestamp('2024-08-01 00:00:00'),
                                         Timestamp('2024-09-01 00:00:00'), Timestamp('2024-10-01 00:00:00'),
                                         Timestamp('2024-11-01 00:00:00'), Timestamp('2024-12-01 00:00:00'),
                                         Timestamp('2025-01-01 00:00:00'), Timestamp('2025-02-01 00:00:00')]})

    df_super_market = pd.DataFrame({'amount': [148.0506997793552, 49.97609992928531, 419.52967186894904, 213.36354599147006, 373.6927281983126, 484.4490023489541, 128.07719256270494, 18.153224419545754, 683.3987542949137, 2461.901330694677, 257.5851908006314, 1447.2810562009415, 794.1885659199669, 1046.4810766134892, 1944.1145159425614, 1587.5624988333052, 1766.5040014119963, 405.649927991727, 2035.5516272119448, 1139.6519476284154, 2902.100638255224, 1850.4546275857165, 2240.310128721645, 681.8331889692474, 1551.634249986425, 2136.906792455504, 648.7521032247125, 4122.733238911268, 275.9079428620795, 3079.6422552697304, 1272.07206321531, 3542.375756405079, 627.8176488538495, 2653.073274366506, 769.9285931567621, 524.8842704235809, 165.68692147522017, 2782.933165720337, 545.9280758924515, 641.7914694657272, 796.9161580276234, 2353.4368365733762, 382.9098815151667, 1844.013324550468, 3025.4219390426297, 3337.342319320241, 810.1390151710284, 1592.9045102183263, 526.0725701789898, 1352.2078755421687, 150.00361509513698, 7.772624872287192, 226.8141158662189, 150.5593087058583, 95.9605867969452, 458.3734893316885, 306.4367667310805, 4025.2826338290165, 1025.6450882472889, 2763.4297188247756, 4065.692166662024, 3112.503787885139, 454.3207753076398, 1683.4268404378936, 629.7114401230565, 203.40847128235228, 1344.0851339023209, 2966.543270137254, 4754.793317581577, 267.7907570632858],
                                    'datetime': [Timestamp('2019-05-01 00:00:00'), Timestamp('2019-06-01 00:00:00'),
                                                 Timestamp('2019-07-01 00:00:00'), Timestamp('2019-08-01 00:00:00'),
                                                 Timestamp('2019-09-01 00:00:00'), Timestamp('2019-10-01 00:00:00'),
                                                 Timestamp('2019-11-01 00:00:00'), Timestamp('2019-12-01 00:00:00'),
                                                 Timestamp('2020-01-01 00:00:00'), Timestamp('2020-02-01 00:00:00'),
                                                 Timestamp('2020-03-01 00:00:00'), Timestamp('2020-04-01 00:00:00'),
                                                 Timestamp('2020-05-01 00:00:00'), Timestamp('2020-06-01 00:00:00'),
                                                 Timestamp('2020-07-01 00:00:00'), Timestamp('2020-08-01 00:00:00'),
                                                 Timestamp('2020-09-01 00:00:00'), Timestamp('2020-10-01 00:00:00'),
                                                 Timestamp('2020-11-01 00:00:00'), Timestamp('2020-12-01 00:00:00'),
                                                 Timestamp('2021-01-01 00:00:00'), Timestamp('2021-02-01 00:00:00'),
                                                 Timestamp('2021-03-01 00:00:00'), Timestamp('2021-04-01 00:00:00'),
                                                 Timestamp('2021-05-01 00:00:00'), Timestamp('2021-06-01 00:00:00'),
                                                 Timestamp('2021-07-01 00:00:00'), Timestamp('2021-08-01 00:00:00'),
                                                 Timestamp('2021-09-01 00:00:00'), Timestamp('2021-10-01 00:00:00'),
                                                 Timestamp('2021-11-01 00:00:00'), Timestamp('2021-12-01 00:00:00'),
                                                 Timestamp('2022-01-01 00:00:00'), Timestamp('2022-02-01 00:00:00'),
                                                 Timestamp('2022-03-01 00:00:00'), Timestamp('2022-04-01 00:00:00'),
                                                 Timestamp('2022-05-01 00:00:00'), Timestamp('2022-06-01 00:00:00'),
                                                 Timestamp('2022-07-01 00:00:00'), Timestamp('2022-08-01 00:00:00'),
                                                 Timestamp('2022-09-01 00:00:00'), Timestamp('2022-10-01 00:00:00'),
                                                 Timestamp('2022-11-01 00:00:00'), Timestamp('2022-12-01 00:00:00'),
                                                 Timestamp('2023-01-01 00:00:00'), Timestamp('2023-02-01 00:00:00'),
                                                 Timestamp('2023-03-01 00:00:00'), Timestamp('2023-04-01 00:00:00'),
                                                 Timestamp('2023-05-01 00:00:00'), Timestamp('2023-06-01 00:00:00'),
                                                 Timestamp('2023-07-01 00:00:00'), Timestamp('2023-08-01 00:00:00'),
                                                 Timestamp('2023-09-01 00:00:00'), Timestamp('2023-10-01 00:00:00'),
                                                 Timestamp('2023-11-01 00:00:00'), Timestamp('2023-12-01 00:00:00'),
                                                 Timestamp('2024-01-01 00:00:00'), Timestamp('2024-02-01 00:00:00'),
                                                 Timestamp('2024-03-01 00:00:00'), Timestamp('2024-04-01 00:00:00'),
                                                 Timestamp('2024-05-01 00:00:00'), Timestamp('2024-06-01 00:00:00'),
                                                 Timestamp('2024-07-01 00:00:00'), Timestamp('2024-08-01 00:00:00'),
                                                 Timestamp('2024-09-01 00:00:00'), Timestamp('2024-10-01 00:00:00'),
                                                 Timestamp('2024-11-01 00:00:00'), Timestamp('2024-12-01 00:00:00'),
                                                 Timestamp('2025-01-01 00:00:00'), Timestamp('2025-02-01 00:00:00')]})

    plot = graphs.multiple_histograms_graph_html(
        amounts=[df_income['amount'], df_rent['amount'], df_super_market['amount']],
        names=['Income', 'Rent', 'Super Market'],
    )

    plot.show()
