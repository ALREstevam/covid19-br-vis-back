# COVID-19 Brazil - data visualization

Using data from [github.com/wcota/covid19br](https://github.com/wcota/covid19br) this project intends to visually show data about the **COVID-19**  in Brazil.

**Note:** this is a personal project  and I'm not a specialist either in data visualization or in disease spreading, so, take it with a grain of salt.


# Endpoints

Currently the API only have two endpoints, so, putting it simply:


## `[GET] http://<...>/api/v1/br/cities`

Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv).

You can use the query param`response_type`, the expected values are `geojson` or `json`, leaving it empty `json` will be used.

[Run on deployed](https://cvid19-back.herokuapp.com/api/v1/br/cities)


## `[GET] http://<...>/api/v1/br/cities-daily`

Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv).

You can use the query param`response_type`, the expected values are `geojson` or `json`, leaving it empty `json` will be used.

[Run on deployed](https://cvid19-back.herokuapp.com/api/v1/br/cities-daily)


## `http://<...>/api/v1/br/cities[-daily]?response_type=json`

[Run on deployed - cities as json](https://cvid19-back.herokuapp.com/api/v1/br/cities?response_type=json)
[Run on deployed - cities daily as json](https://cvid19-back.herokuapp.com/api/v1/br/cities-daily?response_type=json)


The response uses a general json format.

```json
[

    {
        "city": "São Paulo",
        "date": "2020-02-25T00:00:00",
        "deaths": 0,
        "ibgeID": "3550308",
        "location": {
            "lat": -23.5329,
            "long": -46.6395
        },
        "newCases": 1,
        "newDeaths": 0,
        "state": "SP",
        "totalCases": 1
    },
    //...
 ]
```



## `http://<...>/api/v1/br/cities[-daily]?response_type=geojson`

The result uses the geojson format.

[Run on deployed - cities as geojson](https://cvid19-back.herokuapp.com/api/v1/br/cities?response_type=geojson)
[Run on deployed - cities-daily as geojson](https://cvid19-back.herokuapp.com/api/v1/br/cities-daily?response_type=geojson)

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "geometry": {
                "coordinates": [
                    -46.6395,
                    -23.5329
                ],
                "type": "Point"
            },
            "properties": {
                "city": "São Paulo",
                "date": "2020-02-25T00:00:00",
                "deaths": 0,
                "ibgeID": "3550308",
                "newCases": 1,
                "newDeaths": 0,
                "state": "SP",
                "timestamp": 1582588800000,
                "totalCases": 1
            },
            "type": "Feature"
        },
        //...
     ]
  }
```