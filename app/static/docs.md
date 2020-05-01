# COVID-19 Brazil - data visualization

Using data from [github.com/wcota/covid19br](https://github.com/wcota/covid19br) this project intends to visually show data about the **COVID-19**  in Brazil.

**Note:** this is a personal project  and I'm not a specialist either in data visualization or in disease spreading, so, take it with a grain of salt.


# Endpoints

Currently the api has only `[GET]` endpoints, so a easy way to see their behavior is actually making calls.

You need to specify the response format using `.json` or `.geojson`, in the end, it's like requesting a file.
If you use `.geojson` you will receive json data in the geojson specification, using `.json` a generic json format will be used.


## [\[GET\] /api/v1/br/cities.json](../api/v1/br/cities.json)
## [\[GET\] /api/v1/br/cities.geojson](../api/v1/br/cities.geojson)
Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time_changesOnly.csv).

## [\[GET\] /api/v1/br/cities-daily.json](../api/v1/br/cities-daily.json)
## [\[GET\] /api/v1/br/cities-daily.geojson](../api/v1/br/cities-daily.geojson)
Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-cities-time.csv).

## [\[GET\] /api/v1/br/states.json](../api/v1/br/states.json)
## [\[GET\] /api/v1/br/states.json](../api/v1/br/states.json)
Will gather data from: [https://github.com/wcota/covid19br/blob/master/cases-brazil-states.csv](https://github.com/wcota/covid19br/blob/master/cases-brazil-states.csv).
