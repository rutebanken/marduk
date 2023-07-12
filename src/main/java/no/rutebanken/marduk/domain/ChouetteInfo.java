/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package no.rutebanken.marduk.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChouetteInfo {

    private Long id;
    private String xmlns;
    private String xmlnsurl;
    private String referential;
    private String organisation;
    private String user;
    private String dataFormat;
    private boolean enableValidation;
    private boolean allowCreateMissingStopPlace;
    private boolean enableStopPlaceIdMapping;
    private boolean enableCleanImport;
    private boolean enableAutoImport;
    private boolean enableAutoValidation;
    private boolean generateDatedServiceJourneyIds;
    private Set<String> generateMissingServiceLinksForModes;
    private boolean googleUpload;
    private boolean googleQAUpload;
    private boolean enableBlocksExport;

    private Long migrateDataToProvider; // Which dataspace to transfer data to when provider dataspace is valid

    public Long getMigrateDataToProvider() {
        return migrateDataToProvider;
    }

    @Override
    public String toString() {
        return "ChouetteInfo{" +
                       "id=" + id +
                       ", xmlns='" + xmlns + '\'' +
                       ", xmlnsurl='" + xmlnsurl + '\'' +
                       ", referential='" + referential + '\'' +
                       ", organisation='" + organisation + '\'' +
                       ", user='" + user + '\'' +
                       ", dataFormat='" + dataFormat + '\'' +
                       ", enableValidation='" + enableValidation + '\'' +
                       ", allowCreateMissingStopPlace='" + allowCreateMissingStopPlace + '\'' +
                       ", enableStopPlaceIdMapping='" + enableStopPlaceIdMapping + '\'' +
                       ", enableAutoImport='" + enableAutoImport + '\'' +
                       ", enableAutoValidation='" + enableAutoValidation + '\'' +
                       ", enableCleanImport='" + enableCleanImport + '\'' +
                       ", generateMissingServiceLinksForModes='" + generateMissingServiceLinksForModes + '\'' +
                       ", migrateDataToProvider='" + migrateDataToProvider + '\'' +
                       ", generateDatedServiceJourneyIds='" + generateDatedServiceJourneyIds + '\'' +
                       ", googleUpload='" + googleUpload + '\'' +
                       ", googleQAUpload='" + googleQAUpload + '\'' +
                       ", enableBlocksExport='" + enableBlocksExport + '\'' +
                       '}';
    }

    public Long getId() {
        return id;
    }

    public String getXmlns() {
        return xmlns;
    }

    public String getXmlnsurl() {
        return xmlnsurl;
    }

    public String getReferential() {
        return referential;
    }

    public String getOrganisation() {
        return organisation;
    }

    public String getUser() {
        return user;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public boolean isEnableValidation() {
        return enableValidation;
    }

    public boolean isAllowCreateMissingStopPlace() {
        return allowCreateMissingStopPlace;
    }

    public boolean isEnableStopPlaceIdMapping() {
        return enableStopPlaceIdMapping;
    }

    public boolean isEnableCleanImport() {
        return enableCleanImport;
    }

    public boolean isEnableAutoImport() {
        return enableAutoImport;
    }

    public Set<String> getGenerateMissingServiceLinksForModes() {
        return generateMissingServiceLinksForModes;
    }

    public boolean isGenerateDatedServiceJourneyIds() {
        return generateDatedServiceJourneyIds;
    }

    public boolean isGoogleUpload() {
        return googleUpload;
    }

    public boolean isGoogleQAUpload() {
        return googleQAUpload;
    }

    public boolean isEnableAutoValidation() {
        return enableAutoValidation;
    }

    public ChouetteInfo setId(Long id) {
        this.id = id;
        return this;
    }

    public ChouetteInfo setXmlns(String xmlns) {
        this.xmlns = xmlns;
        return this;
    }

    public ChouetteInfo setXmlnsurl(String xmlnsurl) {
        this.xmlnsurl = xmlnsurl;
        return this;
    }

    public ChouetteInfo setReferential(String referential) {
        this.referential = referential;
        return this;
    }

    public ChouetteInfo setOrganisation(String organisation) {
        this.organisation = organisation;
        return this;
    }

    public ChouetteInfo setUser(String user) {
        this.user = user;
        return this;
    }

    public ChouetteInfo setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
        return this;
    }

    public ChouetteInfo setEnableValidation(boolean enableValidation) {
        this.enableValidation = enableValidation;
        return this;
    }

    public ChouetteInfo setAllowCreateMissingStopPlace(boolean allowCreateMissingStopPlace) {
        this.allowCreateMissingStopPlace = allowCreateMissingStopPlace;
        return this;
    }

    public ChouetteInfo setEnableStopPlaceIdMapping(boolean enableStopPlaceIdMapping) {
        this.enableStopPlaceIdMapping = enableStopPlaceIdMapping;
        return this;
    }

    public ChouetteInfo setEnableCleanImport(boolean enableCleanImport) {
        this.enableCleanImport = enableCleanImport;
        return this;
    }

    public ChouetteInfo setEnableAutoImport(boolean enableAutoImport) {
        this.enableAutoImport = enableAutoImport;
        return this;
    }

    public ChouetteInfo setEnableAutoValidation(boolean enableAutoValidation) {
        this.enableAutoValidation = enableAutoValidation;
        return this;
    }

    public ChouetteInfo setGenerateDatedServiceJourneyIds(boolean generateDatedServiceJourneyIds) {
        this.generateDatedServiceJourneyIds = generateDatedServiceJourneyIds;
        return this;
    }

    public ChouetteInfo setGenerateMissingServiceLinksForModes(Set<String> generateMissingServiceLinksForModes) {
        this.generateMissingServiceLinksForModes = generateMissingServiceLinksForModes;
        return this;
    }

    public ChouetteInfo setGoogleUpload(boolean googleUpload) {
        this.googleUpload = googleUpload;
        return this;
    }

    public ChouetteInfo setGoogleQAUpload(boolean googleQAUpload) {
        this.googleQAUpload = googleQAUpload;
        return this;
    }

    public boolean isEnableBlocksExport() {
        return enableBlocksExport;
    }

    public ChouetteInfo setEnableBlocksExport(boolean enableBlocksExport) {
        this.enableBlocksExport = enableBlocksExport;
        return this;
    }

    public ChouetteInfo setMigrateDataToProvider(Long migrateDataToProvider) {
        this.migrateDataToProvider = migrateDataToProvider;
        return this;
    }
}
