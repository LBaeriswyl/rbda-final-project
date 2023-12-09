# Steps to visualize map data in tableau:


1. Load taxi zones data from `taxi_zones.shp`

2. Connect to the presto database as specified in the lecture slides.

3. In the custom SQL query prompt, type `select * from mta_station_data` (without the ;)

4. Go to `Sheet 1` on the bottom menu bar

5. Cmd+click on `Latitude` and `Longitude` from the taxi_zones source on the top left. 

6. Then, add the geometry attribute to the marks box. this will create a map that has the taxi zones visualized. to enable analysis on each zone, drag the `Zone` 
attribute to the `Marks` box.

7. Switch to the Custom SQL query source in the top left. 

8. Cmd+click on `gtfs_latitude` and `gtfs_longitude`, drag it above under `Folders`.

9. Cmd+click on `gtfs_latitude` from the options on the left -> click add to new layer. drag `gtfs_longitude` under the `gtfs_latitude` section in `Marks`.

10. Drag `daytime_routes` and `gtfs_stop_id` to the same section in `Marks`.

11. Click on the dots beside daytime_routes in `Marks` -> click on color to distinguish between routes on the map.
