/opt/anaconda3/envs/graph_database_denormalization/bin/python /Users/yma391/Desktop/M5_home/PhD_research/research/research_2_graph_denormalization/graph_database_denormalization/case_study/embedding/embedding_exp_exd.py 
Running order_customer: Order -[:PLACED_BY]-> Customer
Running order_detail_product: OrderDetail -[:OF_PRODUCT]-> Product
Running order_detail_order: OrderDetail -[:OF_ORDER]-> Order
Running product_supplier: Product -[:SUPPLIED_BY]-> Supplier
Running order_employee: Order -[:HANDLED_BY]-> Employee
Running order_shipper: Order -[:SHIPPED_BY]-> Shipper
Running product_category: Product -[:IN_CATEGORY]-> Category
Running employee_territory_employee: EmployeeTerritory -[:HAS_TERRITORY]-> Employee
Running territory_region: Territory -[:IN_REGION]-> Region

GDS=2026.06.0  dimension=128

[order_customer] Order -[:PLACED_BY]-> Customer
Pairs=4941 (theoretical pairs=4941, eligible parents=88)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.726184    0.808694   0.081642   0.721889    0.805004   0.083115
fast_rp            0.761998    0.643771  -0.117423   0.759671    0.641401  -0.118270
node2vec           0.379763    0.468995   0.055770   0.569129    0.472430  -0.096699
hash_gnn           0.701523    0.668171  -0.030008   0.700981    0.669046  -0.031391
graph_sage         0.993199    0.985797  -0.005044   0.983577    0.957477  -0.026101
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key             | child 1 key       | child 2 key       | norm     | denorm   | norm-denorm | abs-diff
------------+------------------------+-------------------+-------------------+----------+----------+-------------+---------
closest     | {"CustomerID":"RICAR"} | {"OrderID":10299} | {"OrderID":10622} | 0.745911 | 0.745888 | 0.000022    | 0.000022
largest_gap | {"CustomerID":"CACTU"} | {"OrderID":10782} | {"OrderID":10881} | 0.746572 | 0.528703 | 0.217869    | 0.217869
HashGNN tuple details
kind        | child   | tuple                                                                                                                                                                                                                                                                                                                                                                         
------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
closest     | child 1 | {"CustomerID":"RICAR","EmployeeID":4,"Freight":29.76,"OrderDate":"1996-09-06 00:00:00.000","OrderID":10299,"RequiredDate":"1996-10-04 00:00:00.000","ShipAddress":"Av. Copacabana, 267","ShipCity":"Rio de Janeiro","ShipCountry":"Brazil","ShipName":"Ricardo Adocicados","ShipPostalCode":"02389-890","ShipRegion":"RJ","ShipVia":2,"ShippedDate":"1996-09-13 00:00:00.000"}
closest     | child 2 | {"CustomerID":"RICAR","EmployeeID":4,"Freight":50.97,"OrderDate":"1997-08-06 00:00:00.000","OrderID":10622,"RequiredDate":"1997-09-03 00:00:00.000","ShipAddress":"Av. Copacabana, 267","ShipCity":"Rio de Janeiro","ShipCountry":"Brazil","ShipName":"Ricardo Adocicados","ShipPostalCode":"02389-890","ShipRegion":"RJ","ShipVia":3,"ShippedDate":"1997-08-11 00:00:00.000"}
largest_gap | child 1 | {"CustomerID":"CACTU","EmployeeID":9,"Freight":1.1,"OrderDate":"1997-12-17 00:00:00.000","OrderID":10782,"RequiredDate":"1998-01-14 00:00:00.000","ShipAddress":"Cerrito 333","ShipCity":"Buenos Aires","ShipCountry":"Argentina","ShipName":"Cactus Comidas para llevar","ShipPostalCode":"1010","ShipRegion":null,"ShipVia":3,"ShippedDate":"1997-12-22 00:00:00.000"}      
largest_gap | child 2 | {"CustomerID":"CACTU","EmployeeID":4,"Freight":2.84,"OrderDate":"1998-02-11 00:00:00.000","OrderID":10881,"RequiredDate":"1998-03-11 00:00:00.000","ShipAddress":"Cerrito 333","ShipCity":"Buenos Aires","ShipCountry":"Argentina","ShipName":"Cactus Comidas para llevar","ShipPostalCode":"1010","ShipRegion":null,"ShipVia":1,"ShippedDate":"1998-02-18 00:00:00.000"}     

[order_detail_product] OrderDetail -[:OF_PRODUCT]-> Product
Pairs=35446 (theoretical pairs=35446, eligible parents=77)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.456435    0.820513   0.381402   0.421887    0.817243   0.395356
fast_rp            0.746825    0.881021   0.132390   0.742233    0.877943   0.135709
node2vec           0.998178    0.407556  -0.590265   0.997877    0.406889  -0.590988
hash_gnn           0.621719    0.751611   0.125692   0.620987    0.749743   0.127204
graph_sage         0.914794    0.988917   0.057216   0.776424    0.946096   0.169672
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key       | child 1 key                      | child 2 key                      | norm     | denorm   | norm-denorm | abs-diff
------------+------------------+----------------------------------+----------------------------------+----------+----------+-------------+---------
closest     | {"ProductID":44} | {"OrderID":10551,"ProductID":44} | {"OrderID":11034,"ProductID":44} | 0.715318 | 0.715189 | 0.000130    | 0.000130
largest_gap | {"ProductID":19} | {"OrderID":10299,"ProductID":19} | {"OrderID":10847,"ProductID":19} | 0.412022 | 0.754963 | -0.342941   | 0.342941
HashGNN tuple details
kind        | child   | tuple                                                                          
------------+---------+--------------------------------------------------------------------------------
closest     | child 1 | {"Discount":0.0,"OrderID":10551,"ProductID":44,"Quantity":40,"UnitPrice":19.45}
closest     | child 2 | {"Discount":0.0,"OrderID":11034,"ProductID":44,"Quantity":12,"UnitPrice":19.45}
largest_gap | child 1 | {"Discount":0.0,"OrderID":10299,"ProductID":19,"Quantity":15,"UnitPrice":7.3}  
largest_gap | child 2 | {"Discount":0.2,"OrderID":10847,"ProductID":19,"Quantity":12,"UnitPrice":9.2}  

[order_detail_order] OrderDetail -[:OF_ORDER]-> Order
Pairs=2452 (theoretical pairs=2452, eligible parents=693)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.385758    0.923077   0.535979   0.365870    0.920615   0.554745
fast_rp            0.788918    0.973692   0.184318   0.783902    0.973189   0.189287
node2vec           0.429868    0.418474  -0.010921   0.422112    0.413728  -0.008384
hash_gnn           0.575629    0.815614   0.236418   0.575423    0.814252   0.237762
graph_sage         0.997267    0.999961   0.002564   0.987604    0.999907   0.012303
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key        | child 1 key                      | child 2 key                      | norm     | denorm   | norm-denorm | abs-diff
------------+-------------------+----------------------------------+----------------------------------+----------+----------+-------------+---------
closest     | {"OrderID":10938} | {"OrderID":10938,"ProductID":60} | {"OrderID":10938,"ProductID":71} | 0.631891 | 0.713006 | -0.081115   | 0.081115
largest_gap | {"OrderID":11077} | {"OrderID":11077,"ProductID":12} | {"OrderID":11077,"ProductID":4}  | 0.381998 | 0.822815 | -0.440817   | 0.440817
HashGNN tuple details
kind        | child   | tuple                                                                          
------------+---------+--------------------------------------------------------------------------------
closest     | child 1 | {"Discount":0.25,"OrderID":10938,"ProductID":60,"Quantity":49,"UnitPrice":34.0}
closest     | child 2 | {"Discount":0.25,"OrderID":10938,"ProductID":71,"Quantity":35,"UnitPrice":21.5}
largest_gap | child 1 | {"Discount":0.05,"OrderID":11077,"ProductID":12,"Quantity":2,"UnitPrice":38.0} 
largest_gap | child 2 | {"Discount":0.0,"OrderID":11077,"ProductID":4,"Quantity":1,"UnitPrice":22.0}   

[product_supplier] Product -[:SUPPLIED_BY]-> Supplier
Pairs=78 (theoretical pairs=78, eligible parents=26)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.227449    0.687979   0.449331   0.232073    0.693131   0.461059
fast_rp            0.396659    0.467551   0.073809   0.400399    0.469986   0.069587
node2vec           0.434386    0.412506  -0.015665   0.432054    0.415221  -0.016833
hash_gnn           0.633172    0.674879   0.042437   0.632531    0.677039   0.044830
graph_sage         0.834929    0.937667   0.051306   0.738962    0.827838   0.088876
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key        | child 1 key      | child 2 key      | norm     | denorm   | norm-denorm | abs-diff
------------+-------------------+------------------+------------------+----------+----------+-------------+---------
closest     | {"SupplierID":7}  | {"ProductID":17} | {"ProductID":18} | 0.662724 | 0.662624 | 0.000100    | 0.000100
largest_gap | {"SupplierID":15} | {"ProductID":33} | {"ProductID":69} | 0.549694 | 0.729168 | -0.179474   | 0.179474
HashGNN tuple details
kind        | child   | tuple                                                                                                                                                                                                     
------------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
closest     | child 1 | {"CategoryID":6,"Discontinued":true,"ProductID":17,"ProductName":"Alice Mutton","QuantityPerUnit":"20 - 1 kg tins","ReorderLevel":0,"SupplierID":7,"UnitPrice":39.0,"UnitsInStock":0,"UnitsOnOrder":0}    
closest     | child 2 | {"CategoryID":8,"Discontinued":false,"ProductID":18,"ProductName":"Carnarvon Tigers","QuantityPerUnit":"16 kg pkg.","ReorderLevel":0,"SupplierID":7,"UnitPrice":62.5,"UnitsInStock":42,"UnitsOnOrder":0}  
largest_gap | child 1 | {"CategoryID":4,"Discontinued":false,"ProductID":33,"ProductName":"Geitost","QuantityPerUnit":"500 g","ReorderLevel":20,"SupplierID":15,"UnitPrice":2.5,"UnitsInStock":112,"UnitsOnOrder":0}              
largest_gap | child 2 | {"CategoryID":4,"Discontinued":false,"ProductID":69,"ProductName":"Gudbrandsdalsost","QuantityPerUnit":"10 kg pkg.","ReorderLevel":15,"SupplierID":15,"UnitPrice":36.0,"UnitsInStock":26,"UnitsOnOrder":0}

[order_employee] Order -[:HANDLED_BY]-> Employee
Pairs=44041 (theoretical pairs=44041, eligible parents=9)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.490594    0.819760   0.327131   0.487929    0.817975   0.330046
fast_rp            0.699420    0.876199   0.171772   0.697036    0.874270   0.177234
node2vec           0.366446    0.994885   0.628108   0.559662    0.994348   0.434686
hash_gnn           0.612938    0.672397   0.059346   0.612808    0.672477   0.059772
graph_sage         0.996209    0.998101   0.000864   0.987615    0.992852   0.005238
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key       | child 1 key       | child 2 key       | norm     | denorm   | norm-denorm | abs-diff
------------+------------------+-------------------+-------------------+----------+----------+-------------+---------
closest     | {"EmployeeID":3} | {"OrderID":10536} | {"OrderID":10911} | 0.624036 | 0.624035 | 0.000002    | 0.000002
largest_gap | {"EmployeeID":7} | {"OrderID":10336} | {"OrderID":10809} | 0.450131 | 0.720799 | -0.270668   | 0.270668
HashGNN tuple details
kind        | child   | tuple                                                                                                                                                                                                                                                                                                                                                                     
------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
closest     | child 1 | {"CustomerID":"LEHMS","EmployeeID":3,"Freight":58.88,"OrderDate":"1997-05-14 00:00:00.000","OrderID":10536,"RequiredDate":"1997-06-11 00:00:00.000","ShipAddress":"Magazinweg 7","ShipCity":"Frankfurt a.M.","ShipCountry":"Germany","ShipName":"Lehmanns Marktstand","ShipPostalCode":"60528","ShipRegion":null,"ShipVia":2,"ShippedDate":"1997-06-06 00:00:00.000"}     
closest     | child 2 | {"CustomerID":"GODOS","EmployeeID":3,"Freight":38.19,"OrderDate":"1998-02-26 00:00:00.000","OrderID":10911,"RequiredDate":"1998-03-26 00:00:00.000","ShipAddress":"C/ Romero, 33","ShipCity":"Sevilla","ShipCountry":"Spain","ShipName":"Godos Cocina Típica","ShipPostalCode":"41101","ShipRegion":null,"ShipVia":1,"ShippedDate":"1998-03-05 00:00:00.000"}             
largest_gap | child 1 | {"CustomerID":"PRINI","EmployeeID":7,"Freight":15.51,"OrderDate":"1996-10-23 00:00:00.000","OrderID":10336,"RequiredDate":"1996-11-20 00:00:00.000","ShipAddress":"Estrada da saúde n. 58","ShipCity":"Lisboa","ShipCountry":"Portugal","ShipName":"Princesa Isabel Vinhos","ShipPostalCode":"1756","ShipRegion":null,"ShipVia":2,"ShippedDate":"1996-10-25 00:00:00.000"}
largest_gap | child 2 | {"CustomerID":"WELLI","EmployeeID":7,"Freight":4.87,"OrderDate":"1998-01-01 00:00:00.000","OrderID":10809,"RequiredDate":"1998-01-29 00:00:00.000","ShipAddress":"Rua do Mercado, 12","ShipCity":"Resende","ShipCountry":"Brazil","ShipName":"Wellington Importadora","ShipPostalCode":"08737-363","ShipRegion":"SP","ShipVia":1,"ShippedDate":"1998-01-07 00:00:00.000"} 

[order_shipper] Order -[:SHIPPED_BY]-> Shipper
Pairs=100000 (theoretical pairs=116236, eligible parents=3)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.485741    0.556401   0.068217   0.483342    0.554112   0.070770
fast_rp            0.694760    0.573250  -0.120617   0.693040    0.571984  -0.121056
node2vec           0.992477    0.459165  -0.532473   0.991691    0.459882  -0.531809
hash_gnn           0.605379    0.595509  -0.010128   0.605079    0.595420  -0.010280
graph_sage         0.988825    0.817844  -0.160780   0.975775    0.754020  -0.221755
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key      | child 1 key       | child 2 key       | norm     | denorm   | norm-denorm | abs-diff
------------+-----------------+-------------------+-------------------+----------+----------+-------------+---------
closest     | {"ShipperID":2} | {"OrderID":10477} | {"OrderID":10705} | 0.589651 | 0.589651 | -0.000000   | 0.000000
largest_gap | {"ShipperID":2} | {"OrderID":10557} | {"OrderID":10691} | 0.485933 | 0.706248 | -0.220315   | 0.220315
HashGNN tuple details
kind        | child   | tuple                                                                                                                                                                                                                                                                                                                                                                                               
------------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
closest     | child 1 | {"CustomerID":"PRINI","EmployeeID":5,"Freight":13.02,"OrderDate":"1997-03-17 00:00:00.000","OrderID":10477,"RequiredDate":"1997-04-14 00:00:00.000","ShipAddress":"Estrada da saúde n. 58","ShipCity":"Lisboa","ShipCountry":"Portugal","ShipName":"Princesa Isabel Vinhos","ShipPostalCode":"1756","ShipRegion":null,"ShipVia":2,"ShippedDate":"1997-03-25 00:00:00.000"}                          
closest     | child 2 | {"CustomerID":"HILAA","EmployeeID":9,"Freight":3.52,"OrderDate":"1997-10-15 00:00:00.000","OrderID":10705,"RequiredDate":"1997-11-12 00:00:00.000","ShipAddress":"Carrera 22 con Ave. Carlos Soublette #8-35","ShipCity":"San Cristóbal","ShipCountry":"Venezuela","ShipName":"HILARION-Abastos","ShipPostalCode":"5022","ShipRegion":"Táchira","ShipVia":2,"ShippedDate":"1997-11-18 00:00:00.000"}
largest_gap | child 1 | {"CustomerID":"LEHMS","EmployeeID":9,"Freight":96.72,"OrderDate":"1997-06-03 00:00:00.000","OrderID":10557,"RequiredDate":"1997-06-17 00:00:00.000","ShipAddress":"Magazinweg 7","ShipCity":"Frankfurt a.M.","ShipCountry":"Germany","ShipName":"Lehmanns Marktstand","ShipPostalCode":"60528","ShipRegion":null,"ShipVia":2,"ShippedDate":"1997-06-06 00:00:00.000"}                               
largest_gap | child 2 | {"CustomerID":"QUICK","EmployeeID":2,"Freight":810.05,"OrderDate":"1997-10-03 00:00:00.000","OrderID":10691,"RequiredDate":"1997-11-14 00:00:00.000","ShipAddress":"Taucherstraße 10","ShipCity":"Cunewalde","ShipCountry":"Germany","ShipName":"QUICK-Stop","ShipPostalCode":"01307","ShipRegion":null,"ShipVia":2,"ShippedDate":"1997-10-22 00:00:00.000"}                                        

[product_category] Product -[:IN_CATEGORY]-> Category
Pairs=367 (theoretical pairs=367, eligible parents=8)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.190476    0.466850   0.266793   0.194067    0.467905   0.273839
fast_rp            0.381529    0.389769   0.011241   0.383402    0.393337   0.009936
node2vec           0.426980    0.436548   0.005364   0.423694    0.424380   0.000686
hash_gnn           0.583118    0.654368   0.069040   0.587386    0.657766   0.066559
graph_sage         0.454235    0.755366   0.143774   0.456681    0.678379   0.221698
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key       | child 1 key      | child 2 key      | norm     | denorm   | norm-denorm | abs-diff
------------+------------------+------------------+------------------+----------+----------+-------------+---------
closest     | {"CategoryID":4} | {"ProductID":11} | {"ProductID":12} | 0.684802 | 0.685320 | -0.000518   | 0.000518
largest_gap | {"CategoryID":5} | {"ProductID":22} | {"ProductID":23} | 0.547666 | 0.748447 | -0.200781   | 0.200781
HashGNN tuple details
kind        | child   | tuple                                                                                                                                                                                                                  
------------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
closest     | child 1 | {"CategoryID":4,"Discontinued":false,"ProductID":11,"ProductName":"Queso Cabrales","QuantityPerUnit":"1 kg pkg.","ReorderLevel":30,"SupplierID":5,"UnitPrice":21.0,"UnitsInStock":22,"UnitsOnOrder":30}                
closest     | child 2 | {"CategoryID":4,"Discontinued":false,"ProductID":12,"ProductName":"Queso Manchego La Pastora","QuantityPerUnit":"10 - 500 g pkgs.","ReorderLevel":0,"SupplierID":5,"UnitPrice":38.0,"UnitsInStock":86,"UnitsOnOrder":0}
largest_gap | child 1 | {"CategoryID":5,"Discontinued":false,"ProductID":22,"ProductName":"Gustaf's Knäckebröd","QuantityPerUnit":"24 - 500 g pkgs.","ReorderLevel":25,"SupplierID":9,"UnitPrice":21.0,"UnitsInStock":104,"UnitsOnOrder":0}    
largest_gap | child 2 | {"CategoryID":5,"Discontinued":false,"ProductID":23,"ProductName":"Tunnbröd","QuantityPerUnit":"12 - 250 g pkgs.","ReorderLevel":25,"SupplierID":9,"UnitPrice":9.0,"UnitsInStock":61,"UnitsOnOrder":0}                 

[employee_territory_employee] EmployeeTerritory -[:HAS_TERRITORY]-> Employee
Pairs=134 (theoretical pairs=134, eligible parents=9)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.000000    0.986543   0.986510   0.003731    0.984824   0.981092
fast_rp            0.559641    0.999913   0.440244   0.558232    0.999879   0.441647
node2vec           0.600646    0.576958  -0.031079   0.598541    0.556623  -0.041918
hash_gnn           0.534495    0.931017   0.392998   0.533674    0.930085   0.395795
graph_sage         0.985757    0.999999   0.014222   0.960741    0.999994   0.039253
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key       | child 1 key                            | child 2 key                            | norm     | denorm   | norm-denorm | abs-diff
------------+------------------+----------------------------------------+----------------------------------------+----------+----------+-------------+---------
closest     | {"EmployeeID":6} | {"EmployeeID":6,"TerritoryID":"85251"} | {"EmployeeID":6,"TerritoryID":"98052"} | 0.595277 | 0.896648 | -0.301371   | 0.301371
largest_gap | {"EmployeeID":7} | {"EmployeeID":7,"TerritoryID":"95008"} | {"EmployeeID":7,"TerritoryID":"95054"} | 0.459934 | 0.949421 | -0.489487   | 0.489487
HashGNN tuple details
kind        | child   | tuple                                 
------------+---------+---------------------------------------
closest     | child 1 | {"EmployeeID":6,"TerritoryID":"85251"}
closest     | child 2 | {"EmployeeID":6,"TerritoryID":"98052"}
largest_gap | child 1 | {"EmployeeID":7,"TerritoryID":"95008"}
largest_gap | child 2 | {"EmployeeID":7,"TerritoryID":"95054"}

[territory_region] Territory -[:IN_REGION]-> Region
Pairs=359 (theoretical pairs=359, eligible parents=4)
method             norm med  denorm med  delta med  norm mean denorm mean delta mean
property_hash      0.000000    0.500000   0.500000   0.015424    0.503104   0.487680
fast_rp            0.528686    0.107134  -0.413689   0.534781    0.107316  -0.427465
node2vec           0.997336    0.482642  -0.513922   0.997145    0.461907  -0.535238
hash_gnn           0.511769    0.555267   0.046737   0.513006    0.559025   0.050358
graph_sage         0.953636    0.869871  -0.053611   0.891487    0.717051  -0.174437
HashGNN closest and largest gap by abs(Norm - Denorm)
kind        | parent key     | child 1 key             | child 2 key             | norm     | denorm   | norm-denorm | abs-diff
------------+----------------+-------------------------+-------------------------+----------+----------+-------------+---------
closest     | {"RegionID":3} | {"TerritoryID":"48075"} | {"TerritoryID":"48084"} | 0.596630 | 0.596696 | -0.000066   | 0.000066
largest_gap | {"RegionID":1} | {"TerritoryID":"02116"} | {"TerritoryID":"11747"} | 0.437884 | 0.674595 | -0.236712   | 0.236712
HashGNN tuple details
kind        | child   | tuple                                                                   
------------+---------+-------------------------------------------------------------------------
closest     | child 1 | {"RegionID":3,"TerritoryDescription":"Southfield","TerritoryID":"48075"}
closest     | child 2 | {"RegionID":3,"TerritoryDescription":"Troy","TerritoryID":"48084"}      
largest_gap | child 1 | {"RegionID":1,"TerritoryDescription":"Boston","TerritoryID":"02116"}    
largest_gap | child 2 | {"RegionID":1,"TerritoryDescription":"Mellvile","TerritoryID":"11747"}  

Saved: /Users/yma391/Desktop/M5_home/PhD_research/research/research_2_graph_denormalization/graph_database_denormalization/case_study/embedding/embedding_results_exd.json

Process finished with exit code 0
