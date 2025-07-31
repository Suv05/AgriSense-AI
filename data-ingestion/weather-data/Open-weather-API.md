### Current weather data
```bash
https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}
```

- Example response - 

``` json
{
"coord": {
"lon": 10.99,
"lat": 44.34
},
"weather": [
{
"id": 800,
"main": "Clear",
"description": "clear sky",
"icon": "01d"
}
],
"base": "stations",
"main": {
"temp": 295.52,
"feels_like": 295.38,
"temp_min": 295.52,
"temp_max": 295.52,
"pressure": 1016,
"humidity": 60,
"sea_level": 1016,
"grnd_level": 950
},
"visibility": 10000,
"wind": {
"speed": 1.98,
"deg": 21,
"gust": 1.09
},
"clouds": {
"all": 0
},
"dt": 1753950153,
"sys": {
"country": "IT",
"sunrise": 1753934524,
"sunset": 1753987358
},
"timezone": 7200,
"id": 3163858,
"name": "Zocca",
"cod": 200
}
```

### Coordinates by zip/post code
```bash
http://api.openweathermap.org/geo/1.0/zip?zip={zip code},{country code}&appid={API key}
```
- Example response
```json
                
{
  "zip": "90210",
  "name": "Beverly Hills",
  "lat": 34.0901,
  "lon": -118.4065,
  "country": "US"
}
    
```