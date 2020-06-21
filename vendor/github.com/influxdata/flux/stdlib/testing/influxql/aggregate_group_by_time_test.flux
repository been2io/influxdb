package influxql_test

import "testing"
import "internal/influxql"

inData = "
#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,0,,,,,,
,result,table,_time,_measurement,t,_field,_value
,,0,1970-01-01T00:00:00Z,m,0,f,0.2128911717061432
,,0,1970-01-01T01:00:00Z,m,0,f,0.07279997497030395
,,0,1970-01-01T02:00:00Z,m,0,f,0.5685247543217412
,,0,1970-01-01T03:00:00Z,m,0,f,0.29096443393390425
,,0,1970-01-01T04:00:00Z,m,0,f,0.4356705625292191
,,0,1970-01-01T05:00:00Z,m,0,f,0.9377323443071505
,,0,1970-01-01T06:00:00Z,m,0,f,0.011686627909299956
,,0,1970-01-01T07:00:00Z,m,0,f,0.4011512684588352
,,0,1970-01-01T08:00:00Z,m,0,f,0.2467000604224705
,,0,1970-01-01T09:00:00Z,m,0,f,0.035457662801040575
,,0,1970-01-01T10:00:00Z,m,0,f,0.34069878765762024
,,0,1970-01-01T11:00:00Z,m,0,f,0.0956150560348323
,,0,1970-01-01T12:00:00Z,m,0,f,0.6807780291957698
,,0,1970-01-01T13:00:00Z,m,0,f,0.5337946188653447
,,0,1970-01-01T14:00:00Z,m,0,f,0.20254189091820943
,,0,1970-01-01T15:00:00Z,m,0,f,0.9364775207397588
,,0,1970-01-01T16:00:00Z,m,0,f,0.7339764257047982
,,0,1970-01-01T17:00:00Z,m,0,f,0.5833903306697613
,,0,1970-01-01T18:00:00Z,m,0,f,0.3640275286497918
,,0,1970-01-01T19:00:00Z,m,0,f,0.6631189097103277
,,1,1970-01-01T00:00:00Z,m,1,f,0.6769161010724867
,,1,1970-01-01T01:00:00Z,m,1,f,0.6543314107501336
,,1,1970-01-01T02:00:00Z,m,1,f,0.6291463500252206
,,1,1970-01-01T03:00:00Z,m,1,f,0.3152301292534112
,,1,1970-01-01T04:00:00Z,m,1,f,0.03392415665887264
,,1,1970-01-01T05:00:00Z,m,1,f,0.5082438395428922
,,1,1970-01-01T06:00:00Z,m,1,f,0.10525600178569848
,,1,1970-01-01T07:00:00Z,m,1,f,0.09521084883546317
,,1,1970-01-01T08:00:00Z,m,1,f,0.865591485631552
,,1,1970-01-01T09:00:00Z,m,1,f,0.4959724763808837
,,1,1970-01-01T10:00:00Z,m,1,f,0.05679627383763615
,,1,1970-01-01T11:00:00Z,m,1,f,0.1220798530727814
,,1,1970-01-01T12:00:00Z,m,1,f,0.41254260137685644
,,1,1970-01-01T13:00:00Z,m,1,f,0.07588672428630415
,,1,1970-01-01T14:00:00Z,m,1,f,0.24813765601579021
,,1,1970-01-01T15:00:00Z,m,1,f,0.087428932094856
,,1,1970-01-01T16:00:00Z,m,1,f,0.768343862050663
,,1,1970-01-01T17:00:00Z,m,1,f,0.9683960475385988
,,1,1970-01-01T18:00:00Z,m,1,f,0.024459901010390767
,,1,1970-01-01T19:00:00Z,m,1,f,0.16444015119704355
,,2,1970-01-01T00:00:00Z,m,2,f,0.15676131286844733
,,2,1970-01-01T01:00:00Z,m,2,f,0.6400877570876031
,,2,1970-01-01T02:00:00Z,m,2,f,0.893878275849246
,,2,1970-01-01T03:00:00Z,m,2,f,0.7997870081362324
,,2,1970-01-01T04:00:00Z,m,2,f,0.08663328673289308
,,2,1970-01-01T05:00:00Z,m,2,f,0.8342029060820773
,,2,1970-01-01T06:00:00Z,m,2,f,0.11628112203352979
,,2,1970-01-01T07:00:00Z,m,2,f,0.2623494322713019
,,2,1970-01-01T08:00:00Z,m,2,f,0.2803453288904869
,,2,1970-01-01T09:00:00Z,m,2,f,0.1117401998642663
,,2,1970-01-01T10:00:00Z,m,2,f,0.3250295694300974
,,2,1970-01-01T11:00:00Z,m,2,f,0.6078411671373538
,,2,1970-01-01T12:00:00Z,m,2,f,0.2512356097508373
,,2,1970-01-01T13:00:00Z,m,2,f,0.4438471268050767
,,2,1970-01-01T14:00:00Z,m,2,f,0.4961524472008469
,,2,1970-01-01T15:00:00Z,m,2,f,0.27020729375557506
,,2,1970-01-01T16:00:00Z,m,2,f,0.08718790189805248
,,2,1970-01-01T17:00:00Z,m,2,f,0.9737141084388389
,,2,1970-01-01T18:00:00Z,m,2,f,0.07166549074370027
,,2,1970-01-01T19:00:00Z,m,2,f,0.26943653430867687
,,3,1970-01-01T00:00:00Z,m,3,f,0.14437387897405465
,,3,1970-01-01T01:00:00Z,m,3,f,0.21057568804660023
,,3,1970-01-01T02:00:00Z,m,3,f,0.1100457949346592
,,3,1970-01-01T03:00:00Z,m,3,f,0.21141077792261312
,,3,1970-01-01T04:00:00Z,m,3,f,0.19825711934860404
,,3,1970-01-01T05:00:00Z,m,3,f,0.8253021658464177
,,3,1970-01-01T06:00:00Z,m,3,f,0.7540703924924461
,,3,1970-01-01T07:00:00Z,m,3,f,0.9503578735213787
,,3,1970-01-01T08:00:00Z,m,3,f,0.0355914513304335
,,3,1970-01-01T09:00:00Z,m,3,f,0.2745746957286906
,,3,1970-01-01T10:00:00Z,m,3,f,0.5443182488460032
,,3,1970-01-01T11:00:00Z,m,3,f,0.44666597783400147
,,3,1970-01-01T12:00:00Z,m,3,f,0.8093304994650494
,,3,1970-01-01T13:00:00Z,m,3,f,0.4029418644675999
,,3,1970-01-01T14:00:00Z,m,3,f,0.8576441137558692
,,3,1970-01-01T15:00:00Z,m,3,f,0.5687663277465801
,,3,1970-01-01T16:00:00Z,m,3,f,0.7196911732820932
,,3,1970-01-01T17:00:00Z,m,3,f,0.029762405134454425
,,3,1970-01-01T18:00:00Z,m,3,f,0.04147327683312637
,,3,1970-01-01T19:00:00Z,m,3,f,0.9540246504017378
,,4,1970-01-01T00:00:00Z,m,4,f,0.5897122797730547
,,4,1970-01-01T01:00:00Z,m,4,f,0.34947791862749694
,,4,1970-01-01T02:00:00Z,m,4,f,0.0348965616058906
,,4,1970-01-01T03:00:00Z,m,4,f,0.5410660442822854
,,4,1970-01-01T04:00:00Z,m,4,f,0.5905190211554855
,,4,1970-01-01T05:00:00Z,m,4,f,0.26747573301173966
,,4,1970-01-01T06:00:00Z,m,4,f,0.9013278299551851
,,4,1970-01-01T07:00:00Z,m,4,f,0.020645183363589
,,4,1970-01-01T08:00:00Z,m,4,f,0.9444715783077008
,,4,1970-01-01T09:00:00Z,m,4,f,0.9249340696871452
,,4,1970-01-01T10:00:00Z,m,4,f,0.895407127686278
,,4,1970-01-01T11:00:00Z,m,4,f,0.09158865660821072
,,4,1970-01-01T12:00:00Z,m,4,f,0.26170334389270705
,,4,1970-01-01T13:00:00Z,m,4,f,0.04047303046439957
,,4,1970-01-01T14:00:00Z,m,4,f,0.6890175711780648
,,4,1970-01-01T15:00:00Z,m,4,f,0.169203543095355
,,4,1970-01-01T16:00:00Z,m,4,f,0.30352036330937937
,,4,1970-01-01T17:00:00Z,m,4,f,0.7227629347521738
,,4,1970-01-01T18:00:00Z,m,4,f,0.23071325246204685
,,4,1970-01-01T19:00:00Z,m,4,f,0.5423975878441447
,,5,1970-01-01T00:00:00Z,m,5,f,0.6159810548328785
,,5,1970-01-01T01:00:00Z,m,5,f,0.5286371352099966
,,5,1970-01-01T02:00:00Z,m,5,f,0.27037470564664345
,,5,1970-01-01T03:00:00Z,m,5,f,0.9821155970111088
,,5,1970-01-01T04:00:00Z,m,5,f,0.14128644025052767
,,5,1970-01-01T05:00:00Z,m,5,f,0.15532267675041508
,,5,1970-01-01T06:00:00Z,m,5,f,0.06470957508645864
,,5,1970-01-01T07:00:00Z,m,5,f,0.852695602303644
,,5,1970-01-01T08:00:00Z,m,5,f,0.9769235734819968
,,5,1970-01-01T09:00:00Z,m,5,f,0.569772655210167
,,5,1970-01-01T10:00:00Z,m,5,f,0.17839244855342468
,,5,1970-01-01T11:00:00Z,m,5,f,0.5601900079688499
,,5,1970-01-01T12:00:00Z,m,5,f,0.7674758179196
,,5,1970-01-01T13:00:00Z,m,5,f,0.2698164186565066
,,5,1970-01-01T14:00:00Z,m,5,f,0.019420090923226472
,,5,1970-01-01T15:00:00Z,m,5,f,0.2671021005213226
,,5,1970-01-01T16:00:00Z,m,5,f,0.2470396837146283
,,5,1970-01-01T17:00:00Z,m,5,f,0.20522314571010808
,,5,1970-01-01T18:00:00Z,m,5,f,0.47998413047851307
,,5,1970-01-01T19:00:00Z,m,5,f,0.48347993430331904
,,6,1970-01-01T00:00:00Z,m,6,f,0.6342972876998171
,,6,1970-01-01T01:00:00Z,m,6,f,0.6576024605878982
,,6,1970-01-01T02:00:00Z,m,6,f,0.6447000482746346
,,6,1970-01-01T03:00:00Z,m,6,f,0.21678100598418243
,,6,1970-01-01T04:00:00Z,m,6,f,0.023408287556663337
,,6,1970-01-01T05:00:00Z,m,6,f,0.9076309223942498
,,6,1970-01-01T06:00:00Z,m,6,f,0.5817502777240137
,,6,1970-01-01T07:00:00Z,m,6,f,0.052824998643205875
,,6,1970-01-01T08:00:00Z,m,6,f,0.3809330972535029
,,6,1970-01-01T09:00:00Z,m,6,f,0.7671686650796129
,,6,1970-01-01T10:00:00Z,m,6,f,0.07979065684298921
,,6,1970-01-01T11:00:00Z,m,6,f,0.3998772862617565
,,6,1970-01-01T12:00:00Z,m,6,f,0.1155491863199121
,,6,1970-01-01T13:00:00Z,m,6,f,0.7386676189881027
,,6,1970-01-01T14:00:00Z,m,6,f,0.34629618068681484
,,6,1970-01-01T15:00:00Z,m,6,f,0.5525776529918931
,,6,1970-01-01T16:00:00Z,m,6,f,0.332627727109297
,,6,1970-01-01T17:00:00Z,m,6,f,0.5131953610405185
,,6,1970-01-01T18:00:00Z,m,6,f,0.6424768197911961
,,6,1970-01-01T19:00:00Z,m,6,f,0.7165748159282228
,,7,1970-01-01T00:00:00Z,m,7,f,0.2455426279704188
,,7,1970-01-01T01:00:00Z,m,7,f,0.40163124489623003
,,7,1970-01-01T02:00:00Z,m,7,f,0.5975613157276424
,,7,1970-01-01T03:00:00Z,m,7,f,0.24368494285955775
,,7,1970-01-01T04:00:00Z,m,7,f,0.5064758608991188
,,7,1970-01-01T05:00:00Z,m,7,f,0.8752680628605042
,,7,1970-01-01T06:00:00Z,m,7,f,0.6073886500434565
,,7,1970-01-01T07:00:00Z,m,7,f,0.38193775998692464
,,7,1970-01-01T08:00:00Z,m,7,f,0.39598277949908883
,,7,1970-01-01T09:00:00Z,m,7,f,0.15966785725202795
,,7,1970-01-01T10:00:00Z,m,7,f,0.629484974659171
,,7,1970-01-01T11:00:00Z,m,7,f,0.8986665286614761
,,7,1970-01-01T12:00:00Z,m,7,f,0.8351669026338405
,,7,1970-01-01T13:00:00Z,m,7,f,0.7207824488925798
,,7,1970-01-01T14:00:00Z,m,7,f,0.5707878569829702
,,7,1970-01-01T15:00:00Z,m,7,f,0.8091445743234214
,,7,1970-01-01T16:00:00Z,m,7,f,0.3371203534810527
,,7,1970-01-01T17:00:00Z,m,7,f,0.1741931226422866
,,7,1970-01-01T18:00:00Z,m,7,f,0.7377303139536953
,,7,1970-01-01T19:00:00Z,m,7,f,0.6414830272020358
,,8,1970-01-01T00:00:00Z,m,8,f,0.507272964067779
,,8,1970-01-01T01:00:00Z,m,8,f,0.7119639952021554
,,8,1970-01-01T02:00:00Z,m,8,f,0.811656300965649
,,8,1970-01-01T03:00:00Z,m,8,f,0.42116179653493335
,,8,1970-01-01T04:00:00Z,m,8,f,0.43222575065281893
,,8,1970-01-01T05:00:00Z,m,8,f,0.5074074618881453
,,8,1970-01-01T06:00:00Z,m,8,f,0.5122144565697357
,,8,1970-01-01T07:00:00Z,m,8,f,0.40715470738979853
,,8,1970-01-01T08:00:00Z,m,8,f,0.8185589852847821
,,8,1970-01-01T09:00:00Z,m,8,f,0.06615362288768847
,,8,1970-01-01T10:00:00Z,m,8,f,0.95397955896684
,,8,1970-01-01T11:00:00Z,m,8,f,0.3012853054957797
,,8,1970-01-01T12:00:00Z,m,8,f,0.6957865828883222
,,8,1970-01-01T13:00:00Z,m,8,f,0.18349784531489438
,,8,1970-01-01T14:00:00Z,m,8,f,0.5326579864159198
,,8,1970-01-01T15:00:00Z,m,8,f,0.16264304295239912
,,8,1970-01-01T16:00:00Z,m,8,f,0.39612399186342956
,,8,1970-01-01T17:00:00Z,m,8,f,0.8801431671211721
,,8,1970-01-01T18:00:00Z,m,8,f,0.8905454115467667
,,8,1970-01-01T19:00:00Z,m,8,f,0.37008895622309546
,,9,1970-01-01T00:00:00Z,m,9,f,0.5943978030122283
,,9,1970-01-01T01:00:00Z,m,9,f,0.9729045142730391
,,9,1970-01-01T02:00:00Z,m,9,f,0.29963260412871184
,,9,1970-01-01T03:00:00Z,m,9,f,0.13549246068853443
,,9,1970-01-01T04:00:00Z,m,9,f,0.4985352282527366
,,9,1970-01-01T05:00:00Z,m,9,f,0.70041974640892
,,9,1970-01-01T06:00:00Z,m,9,f,0.5505166729368712
,,9,1970-01-01T07:00:00Z,m,9,f,0.529948574619832
,,9,1970-01-01T08:00:00Z,m,9,f,0.591106104564076
,,9,1970-01-01T09:00:00Z,m,9,f,0.0635615209104685
,,9,1970-01-01T10:00:00Z,m,9,f,0.49844667852584
,,9,1970-01-01T11:00:00Z,m,9,f,0.8229130635259402
,,9,1970-01-01T12:00:00Z,m,9,f,0.5768314206131357
,,9,1970-01-01T13:00:00Z,m,9,f,0.7964902809518639
,,9,1970-01-01T14:00:00Z,m,9,f,0.8577185880621226
,,9,1970-01-01T15:00:00Z,m,9,f,0.8046611697264398
,,9,1970-01-01T16:00:00Z,m,9,f,0.9035778001333393
,,9,1970-01-01T17:00:00Z,m,9,f,0.25310651193805
,,9,1970-01-01T18:00:00Z,m,9,f,0.9644663191492964
,,9,1970-01-01T19:00:00Z,m,9,f,0.41841409390509593
,,10,1970-01-01T00:00:00Z,m,a,f,0.18615629501863318
,,10,1970-01-01T01:00:00Z,m,a,f,0.07946862666753376
,,10,1970-01-01T02:00:00Z,m,a,f,0.33322548107896877
,,10,1970-01-01T03:00:00Z,m,a,f,0.6074227273766635
,,10,1970-01-01T04:00:00Z,m,a,f,0.8355600086380185
,,10,1970-01-01T05:00:00Z,m,a,f,0.0696815215736273
,,10,1970-01-01T06:00:00Z,m,a,f,0.9745054585018766
,,10,1970-01-01T07:00:00Z,m,a,f,0.2845881026557506
,,10,1970-01-01T08:00:00Z,m,a,f,0.38922817710857965
,,10,1970-01-01T09:00:00Z,m,a,f,0.5614403393810139
,,10,1970-01-01T10:00:00Z,m,a,f,0.5197270817554469
,,10,1970-01-01T11:00:00Z,m,a,f,0.021532590173884557
,,10,1970-01-01T12:00:00Z,m,a,f,0.16056999815441234
,,10,1970-01-01T13:00:00Z,m,a,f,0.9518781786152178
,,10,1970-01-01T14:00:00Z,m,a,f,0.27274707738681897
,,10,1970-01-01T15:00:00Z,m,a,f,0.3202684728841677
,,10,1970-01-01T16:00:00Z,m,a,f,0.30600536680315443
,,10,1970-01-01T17:00:00Z,m,a,f,0.7144229319519285
,,10,1970-01-01T18:00:00Z,m,a,f,0.8195988405538475
,,10,1970-01-01T19:00:00Z,m,a,f,0.6833069146305664
,,11,1970-01-01T00:00:00Z,m,b,f,0.08212464969207754
,,11,1970-01-01T01:00:00Z,m,b,f,0.7049389293987478
,,11,1970-01-01T02:00:00Z,m,b,f,0.46150233142660235
,,11,1970-01-01T03:00:00Z,m,b,f,0.3061287983538073
,,11,1970-01-01T04:00:00Z,m,b,f,0.5095622098330529
,,11,1970-01-01T05:00:00Z,m,b,f,0.24885143749146582
,,11,1970-01-01T06:00:00Z,m,b,f,0.2720705701013104
,,11,1970-01-01T07:00:00Z,m,b,f,0.831725266287822
,,11,1970-01-01T08:00:00Z,m,b,f,0.2980839741707451
,,11,1970-01-01T09:00:00Z,m,b,f,0.4667397329742896
,,11,1970-01-01T10:00:00Z,m,b,f,0.7604282480092655
,,11,1970-01-01T11:00:00Z,m,b,f,0.08125558409370949
,,11,1970-01-01T12:00:00Z,m,b,f,0.9673492809150086
,,11,1970-01-01T13:00:00Z,m,b,f,0.7485101477006051
,,11,1970-01-01T14:00:00Z,m,b,f,0.7826905277143607
,,11,1970-01-01T15:00:00Z,m,b,f,0.002832539681341695
,,11,1970-01-01T16:00:00Z,m,b,f,0.5904945620548707
,,11,1970-01-01T17:00:00Z,m,b,f,0.19377318954716558
,,11,1970-01-01T18:00:00Z,m,b,f,0.32112472445570694
,,11,1970-01-01T19:00:00Z,m,b,f,0.8156620813866876
,,12,1970-01-01T00:00:00Z,m,c,f,0.45823190604583425
,,12,1970-01-01T01:00:00Z,m,c,f,0.7041395377467482
,,12,1970-01-01T02:00:00Z,m,c,f,0.7164281522457248
,,12,1970-01-01T03:00:00Z,m,c,f,0.664229117231648
,,12,1970-01-01T04:00:00Z,m,c,f,0.2167198068478531
,,12,1970-01-01T05:00:00Z,m,c,f,0.4781537327645974
,,12,1970-01-01T06:00:00Z,m,c,f,0.915856481062239
,,12,1970-01-01T07:00:00Z,m,c,f,0.7488973719504495
,,12,1970-01-01T08:00:00Z,m,c,f,0.8415102413049199
,,12,1970-01-01T09:00:00Z,m,c,f,0.9749127169681439
,,12,1970-01-01T10:00:00Z,m,c,f,0.5203067180352847
,,12,1970-01-01T11:00:00Z,m,c,f,0.8077896981284608
,,12,1970-01-01T12:00:00Z,m,c,f,0.3140632603734003
,,12,1970-01-01T13:00:00Z,m,c,f,0.4942913283054576
,,12,1970-01-01T14:00:00Z,m,c,f,0.7803195300512884
,,12,1970-01-01T15:00:00Z,m,c,f,0.29988813201194514
,,12,1970-01-01T16:00:00Z,m,c,f,0.9275317190485068
,,12,1970-01-01T17:00:00Z,m,c,f,0.8532168145174167
,,12,1970-01-01T18:00:00Z,m,c,f,0.29567768993242205
,,12,1970-01-01T19:00:00Z,m,c,f,0.148869586329582
,,13,1970-01-01T00:00:00Z,m,d,f,0.8734370766635879
,,13,1970-01-01T01:00:00Z,m,d,f,0.7450766293779155
,,13,1970-01-01T02:00:00Z,m,d,f,0.9454605686978569
,,13,1970-01-01T03:00:00Z,m,d,f,0.20587883115663197
,,13,1970-01-01T04:00:00Z,m,d,f,0.5495265257832065
,,13,1970-01-01T05:00:00Z,m,d,f,0.9985101041430109
,,13,1970-01-01T06:00:00Z,m,d,f,0.606668520981593
,,13,1970-01-01T07:00:00Z,m,d,f,0.1520034163165451
,,13,1970-01-01T08:00:00Z,m,d,f,0.8960639437047637
,,13,1970-01-01T09:00:00Z,m,d,f,0.40243673434606525
,,13,1970-01-01T10:00:00Z,m,d,f,0.8559633842216021
,,13,1970-01-01T11:00:00Z,m,d,f,0.8049602305582066
,,13,1970-01-01T12:00:00Z,m,d,f,0.7472498943010795
,,13,1970-01-01T13:00:00Z,m,d,f,0.19955816724612416
,,13,1970-01-01T14:00:00Z,m,d,f,0.6398800958352263
,,13,1970-01-01T15:00:00Z,m,d,f,0.9121026864646193
,,13,1970-01-01T16:00:00Z,m,d,f,0.5138919840212206
,,13,1970-01-01T17:00:00Z,m,d,f,0.8090917843470073
,,13,1970-01-01T18:00:00Z,m,d,f,0.7569845252265965
,,13,1970-01-01T19:00:00Z,m,d,f,0.28321607497510914
,,14,1970-01-01T00:00:00Z,m,e,f,0.015015322322770903
,,14,1970-01-01T01:00:00Z,m,e,f,0.9355541025597022
,,14,1970-01-01T02:00:00Z,m,e,f,0.04136655165051055
,,14,1970-01-01T03:00:00Z,m,e,f,0.5818917517534496
,,14,1970-01-01T04:00:00Z,m,e,f,0.5003101543125776
,,14,1970-01-01T05:00:00Z,m,e,f,0.9100689489842115
,,14,1970-01-01T06:00:00Z,m,e,f,0.16048736645104625
,,14,1970-01-01T07:00:00Z,m,e,f,0.9463952890222139
,,14,1970-01-01T08:00:00Z,m,e,f,0.9639066785470444
,,14,1970-01-01T09:00:00Z,m,e,f,0.5265981043058684
,,14,1970-01-01T10:00:00Z,m,e,f,0.1618974442468811
,,14,1970-01-01T11:00:00Z,m,e,f,0.23732885629224745
,,14,1970-01-01T12:00:00Z,m,e,f,0.6209913661783305
,,14,1970-01-01T13:00:00Z,m,e,f,0.37725772933735735
,,14,1970-01-01T14:00:00Z,m,e,f,0.8852154063171275
,,14,1970-01-01T15:00:00Z,m,e,f,0.7614955446339434
,,14,1970-01-01T16:00:00Z,m,e,f,0.8089264477070236
,,14,1970-01-01T17:00:00Z,m,e,f,0.8491087096738495
,,14,1970-01-01T18:00:00Z,m,e,f,0.051500691978027605
,,14,1970-01-01T19:00:00Z,m,e,f,0.16249922364133557
"

outData = "
#datatype,string,long,dateTime:RFC3339,string,double
#group,false,false,false,true,false
#default,0,,,,
,result,table,time,_measurement,sum
,,0,1970-01-01T00:00:00Z,m,33.802561295632636
,,0,1970-01-01T05:00:00Z,m,38.603075816590454
,,0,1970-01-01T10:00:00Z,m,36.99580140544222
,,0,1970-01-01T15:00:00Z,m,37.63380091958056
,,0,1970-01-01T20:00:00Z,m,
"

// SELECT sum(f) FROM m WHERE time >= 0 AND time <= 20h GROUP BY time(5h)
t_aggregate_group_by_time = (tables=<-) => tables
	|> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T20:00:00.000000001Z)
	|> filter(fn: (r) => r._measurement == "m")
	|> filter(fn: (r) => r._field == "f")
	|> group(columns: ["_measurement", "_field"])
	|> aggregateWindow(every: 5h, fn: sum, timeSrc: "_start")
	|> rename(columns: {_time: "time", _value: "sum"})
	|> drop(columns: ["_field", "_start", "_stop"])

test _aggregate_group_by_time = () => ({
	input: testing.loadStorage(csv: inData),
	want: testing.loadMem(csv: outData),
	fn: t_aggregate_group_by_time,
})
