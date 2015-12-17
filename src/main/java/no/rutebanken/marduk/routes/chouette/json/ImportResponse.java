package no.rutebanken.marduk.routes.chouette.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ImportResponse {

    public enum Status {SCHEDULED, STARTED, TERMINATED, CANCELED, ABORTED}

    public Integer id;
    public String referential;
    public String action;
    public String type;
    public Long created;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Long started;
    public Long updated;
    public Status status;
    public List<LinkInfo> links;
    @JsonProperty("action_parameters")
    public ActionParameters actionParameters;

    public static class LinkInfo {
        public String rel;
        public String href;
        public String type;
        public String method;
    }

    public static class ActionParameters {

        public String name;

        @JsonProperty("no_save")
        public boolean noSave;

        @JsonProperty("user_name")
        public String userName;

        @JsonProperty("organisation_name")
        public String organisationName;

        @JsonProperty("referential_name")
        public String referentialName;

        @JsonProperty("object_id_prefix")
        public String objectIdPrefix;

        @JsonProperty("max_distance_for_commercial")
        public Integer maxDistanceForCommercial;

        @JsonProperty("ignore_last_word")
        public boolean ignoreLastWord;

        @JsonProperty("ignore_end_chars")
        public Integer ignoreEndChars;

        @JsonProperty("max_distance_for_connection_link")
        @JsonInclude(JsonInclude.Include.ALWAYS)
        public Integer maxDistanceForConnectionLink;

        @JsonProperty("references_type")
        @JsonInclude(JsonInclude.Include.ALWAYS)
        public String referencesType = "";

        @JsonProperty("clean_repository")
        public boolean cleanRepository;

    }

}
