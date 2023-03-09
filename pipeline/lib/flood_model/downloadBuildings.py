
def downoadGoogleBildings():
    import urllib.request
    from geojson import Feature, FeatureCollection, Point
    import gzip
    import csv

    urls=['https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/32f_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/33d_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/325_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/331_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/339_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/33b_buildings.csv.gz',
    'https://storage.googleapis.com/open-buildings-data/v2/points_s2_level_4_gzip/323_buildings.csv.gz']




    features = []

    fnames=[]
    
    for url in urls:
        filename=url.split('/')[-1]
        fnames.append(f'./data/other/input/building footprint/{filename}')
        print(url)
        urllib.request.urlretrieve(url, f'./data/other/input/building footprint/{filename}')
        

            
    for filename in fnames:     
        jsonfilename=filename.split('/')[-1].split('.')[0]
        with gzip.open(filename, 'rt') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                latitude, longitude,area,confidence = map(float, (row['latitude'], row['longitude'],row['area_in_meters'],row['confidence']))
                features.append(
                    Feature(
                        geometry = Point((longitude, latitude)),
                        properties = {
                        'area_in_meters': area,
                        'confidence': confidence,
                        }

                    )
                )

                
        collection = FeatureCollection(features)
        with open(f"./data/other/input/building footprint/{jsonfilename}.geojson", "w") as f:
            f.write('%s' % collection)