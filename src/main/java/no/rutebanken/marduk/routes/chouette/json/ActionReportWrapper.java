package no.rutebanken.marduk.routes.chouette.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ActionReportWrapper {

    /*

    {"action_report": {
  "progression": {
    "current_step": 1,
    "steps_count": 3,
    "steps": [
      {
        "step": "INITIALISATION",
        "total": 5,
        "realized": 4
      },
      {
        "step": "PROCESSING",
        "total": 1,
        "realized": 0
      },
      {
        "step": "FINALISATION",
        "total": 1,
        "realized": 0
      }
    ]
  },
  "result": "NOK",
  "zip_file": {
    "name": "20151217143403-000038-gtfs.zip",
    "status": "OK"
  },
  "files": [
    {
      "name": "stop_times.txt",
      "status": "IGNORED"
    },
    {
      "name": "agency.txt",
      "status": "ERROR",
      "errors": [{
        "code": "READ_ERROR",
        "description": "Il y a des erreurs dans ce fichier."
      }]
    },
    {
      "name": "routes.txt",
      "status": "ERROR",
      "errors": [{
        "code": "READ_ERROR",
        "description": "Il y a des erreurs dans ce fichier."
      }]
    },
    {
      "name": "stops.txt",
      "status": "IGNORED"
    },
    {
      "name": "calendar_dates.txt",
      "status": "IGNORED"
    },
    {
      "name": "trips.txt",
      "status": "IGNORED"
    },
    {
      "name": "calendar.txt",
      "status": "IGNORED"
    }
  ],
  "stats": {
    "line_count": 0,
    "route_count": 0,
    "connection_link_count": 0,
    "time_table_count": 0,
    "stop_area_count": 0,
    "access_point_count": 0,
    "vehicle_journey_count": 0,
    "journey_pattern_count": 0
  },
  "failure": {
    "code": "INVALID_DATA",
    "description": "INVALID_FORMAT \/opt\/jboss\/referentials\/tds\/data\/6\/input\/routes.txt"
  }
}}

     */

    @JsonProperty("action_report")
    public ActionReport actionReport;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ActionReport {
       public String result;
    }

}
