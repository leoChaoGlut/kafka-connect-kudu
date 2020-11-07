from fabric import SerialGroup

result = SerialGroup(
    'mgmt01',
    'datamaster01',
    'dataslave01',
    'dataslave02',
    'dataslave03',
    'etl01',
    'etl02',
    'etl03',
    'etl04',
    'etl05',
    'etl06',
    'query01',
    'query02',
    'query03',
    'query04',
    'query05',
    'query06',
    'query07',
    'query08',
    'kudu1',
    'kudu2',
    'kudu3',
    'mq1',
    'mq2',
    'mq3'
).run('yum install -y unzip gcc* ntp mysql-connector-java*')

for res in result.items():
    print(res)
