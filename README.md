# COVID-19 Brazil - data visualization

Using data from [github.com/wcota/covid19br](https://github.com/wcota/covid19br) this project intends to visually show data about the **COVID-19**  in Brazil.

**Note:** this is a personal project  and I'm not a specialist either in data visualization or in disease spreading, so, take it with a grain of salt.

**Deployed at:** [https://cvid19-back.herokuapp.com/](https://cvid19-back.herokuapp.com/)

![br1](README.assets/br1-1587419459773.gif)



![br2](README.assets/br2.gif)



![sp](README.assets/sp.gif)



# Endpoints

Currently the API only have two endpoints, so, putting it simply:


## `[GET] <address>/br/cities`

Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv).

You can use the query param`response_type`, the expected values are `geojson` or `json`, leaving it empty `json` will be used.



## `[GET] <address>/br/cities-daily`

Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv).

You can use the query param`response_type`, the expected values are `geojson` or `json`, leaving it empty `json` will be used.



## `<address>/br/cities[-daily]?response_type=json`

The response uses a general json fromat.

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



## `<address>/br/cities[-daily]?response_type=geojson`

The result uses the geojson format.

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
