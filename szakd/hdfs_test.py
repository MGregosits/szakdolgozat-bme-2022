import pyhdfs

fs = pyhdfs.HdfsClient(hosts='localhost:9870', user_name='dr.who')

fs.copy_from_local('/home/gmate/szakd/mapmatched_data/2019_03_10__23_57_58__2019_03_17__23_57_58.csv', '/user/web/output/')