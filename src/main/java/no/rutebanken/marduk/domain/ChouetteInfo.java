package no.rutebanken.marduk.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChouetteInfo {

    public Long id;
    public String prefix;
    public String dataSpace;
    public String organisation;
    public String user;
    public String regtoppVersion;
    public String regtoppCoordinateProjection;

    @Override
    public String toString() {
        return "ChouetteInfo{" +
                "id=" + id +
                ", prefix='" + prefix + '\'' +
                ", dataSpace='" + dataSpace + '\'' +
                ", organisation='" + organisation + '\'' +
                ", user='" + user + '\'' +
                ", regtoppVersion='" + regtoppVersion + '\'' +
                ", regtoppCoordinateProjection='" + regtoppCoordinateProjection + '\'' +
                '}';
    }

    public boolean usesRegtopp(){
        return regtoppVersion != null;
    }

}
