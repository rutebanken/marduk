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

package no.rutebanken.marduk.routes.gtfs;

import no.rutebanken.marduk.routes.file.GtfsFileUtils;
import no.rutebanken.marduk.routes.file.ZipFileUtils;
import no.rutebanken.marduk.routes.google.GoogleRouteTypeCode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GtfsTransformationServiceTest {

    private static final String GTFS_FILE_EXTENDED_ROUTE_TYPES = "src/test/resources/no/rutebanken/marduk/routes/gtfs/extended_gtfs.zip";

    @Test
    public void transformToGoogleFormatExcludeShapes() throws Exception {
        File out = new GtfsTransformationService().transformToGoogleFormat(new File(GTFS_FILE_EXTENDED_ROUTE_TYPES), false);

        FileUtils.copyFile(out, new File("target/test.zip"));

        assertRouteRouteTypesAreConvertedToGoogleSupportedValues(out);
        assertStopVehicleTypesAreConvertedToGoogleSupportedValues(out);

        assertShapesAreRemoved(out);
    }

    @Test
    public void transformToGoogleFormatIncludeShapes() throws Exception {
        File out = new GtfsTransformationService().transformToGoogleFormat(new File(GTFS_FILE_EXTENDED_ROUTE_TYPES), true);

        FileUtils.copyFile(out, new File("target/test.zip"));

        assertRouteRouteTypesAreConvertedToGoogleSupportedValues(out);
        assertStopVehicleTypesAreConvertedToGoogleSupportedValues(out);

        assertShapesAreIncluded(out);
    }


    @Test
    public void transformToBasicGTFSFormatExcludeShapes() throws Exception {
        File out = new GtfsTransformationService().transformToBasicGTFSFormat(new File(GTFS_FILE_EXTENDED_ROUTE_TYPES), false);

        FileUtils.copyFile(out, new File("target/test.zip"));

        assertRouteRouteTypesAreConvertedToBasicGtfsValues(out);
        assertStopVehicleTypesAreConvertedToBasicGtfsValues(out);

        assertShapesAreRemoved(out);
    }

    @Test
    public void transformToBasicGTFSFormatIncludeShapes() throws Exception {
        File out = new GtfsTransformationService().transformToBasicGTFSFormat(new File(GTFS_FILE_EXTENDED_ROUTE_TYPES), true);

        FileUtils.copyFile(out, new File("target/test.zip"));

        assertRouteRouteTypesAreConvertedToBasicGtfsValues(out);
        assertStopVehicleTypesAreConvertedToBasicGtfsValues(out);

        assertShapesAreIncluded(out);
    }

    public static void assertRouteRouteTypesAreConvertedToGoogleSupportedValues(File out) throws IOException {
        List<String> routeLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "routes.txt")), StandardCharsets.UTF_8);
        routeLines.remove(0); // remove header
        assertEquals(10, routeLines.size());

        List<String> transformedRouteTypes = routeLines.stream().map(routeLine -> routeLine.split(",")[4]).collect(Collectors.toList());
        assertThat(transformedRouteTypes.stream().allMatch(routeType -> GoogleRouteTypeCode.fromCode(Integer.valueOf(routeType)) != null)).as("Expected all route types to have been converted to google valid codes").isTrue();
        assertEquals("200", transformedRouteTypes.get(0));
        assertEquals("201", transformedRouteTypes.get(1));
        assertEquals("200", transformedRouteTypes.get(2));
        assertEquals("1501", transformedRouteTypes.get(3));
    }

    public static void assertStopVehicleTypesAreConvertedToGoogleSupportedValues(File out) throws IOException {
        List<String> stopLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "stops.txt")), StandardCharsets.UTF_8);
        stopLines.remove(0); // remove header
        assertThat(stopLines.get(0)).as("Line without vehicle type should not be changed").endsWith(",");
        assertThat(stopLines.get(1)).as("Line with valid value 701 should be kept").endsWith(",701");
        assertThat(stopLines.get(2)).as("Line with extended value 1012 should be converted to 1000").endsWith(",1000");
        assertThat(stopLines.get(3)).as("Line with extended value 1601 should be converted to 1700 (default)").endsWith(",1700");
        List<String> feedInfoLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, GtfsFileUtils.FEED_INFO_FILE_NAME)), StandardCharsets.UTF_8);
        assertThat(feedInfoLines.get(1)).as("Entur info should be used as feed info").isEqualTo("ENTUR,Entur,https://www.entur.org,no");
    }

    public static void assertRouteRouteTypesAreConvertedToBasicGtfsValues(File out) throws IOException {
        List<String> routeLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "routes.txt")), StandardCharsets.UTF_8);
        routeLines.remove(0); // remove header
        assertEquals(10, routeLines.size());

        List<String> transformedRouteTypes = routeLines.stream().map(routeLine -> routeLine.split(",")[4]).collect(Collectors.toList());
        assertThat(transformedRouteTypes.stream().allMatch(routeType -> Arrays.stream(BasicRouteTypeCode.values()).anyMatch(basic -> Integer.toString(basic.getCode()).equals(routeType)))).as("Expected all route types to have been converted to basic codes").isTrue();
        assertEquals("3", transformedRouteTypes.get(0));
        assertEquals("3", transformedRouteTypes.get(1));
        assertEquals("3", transformedRouteTypes.get(2));
        assertEquals("3", transformedRouteTypes.get(3));
    }

    public static void assertStopVehicleTypesAreConvertedToBasicGtfsValues(File out) throws IOException {
        List<String> stopLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "stops.txt")), StandardCharsets.UTF_8);
        stopLines.remove(0); // remove header
        assertThat(stopLines.get(1)).as("Line with valid value 701 should be converted to 3").endsWith(",3");
        assertThat(stopLines.get(2)).as("Line with extended value 1012 should be converted to 4").endsWith(",4");
        assertThat(stopLines.get(3)).as("Line with extended value 1601 should be converted to 3 (default)").endsWith(",3");
        List<String> feedInfoLines = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, GtfsFileUtils.FEED_INFO_FILE_NAME)), StandardCharsets.UTF_8);
        assertThat(feedInfoLines.get(1)).as("Entur info should be used as feed info").isEqualTo("ENTUR,Entur,https://www.entur.org,no");
    }

    public static void assertShapesAreRemoved(File out) throws IOException {
        assertThat(ZipFileUtils.extractFileFromZipFile(out, "shapes.txt")).as("All Shapes should have been removed").isNull();
        ;

        List<String> trips = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "trips.txt")), StandardCharsets.UTF_8);
        String tripWithShape = trips.stream().filter(t -> t.startsWith("9797,262919,")).findFirst().get();

        assertFalse(tripWithShape.contains("SKY:JourneyPattern:1-362_0_10446_1"));
    }

    public static void assertShapesAreIncluded(File out) throws IOException {
        assertThat(ZipFileUtils.extractFileFromZipFile(out, "shapes.txt")).as("All Shapes should be present").isNotNull();
        ;

        List<String> trips = IOUtils.readLines(new ByteArrayInputStream(ZipFileUtils.extractFileFromZipFile(out, "trips.txt")), StandardCharsets.UTF_8);
        String tripWithShape = trips.stream().filter(t -> t.startsWith("9797,262919,")).findFirst().get();

        assertTrue(tripWithShape.contains("SKY:JourneyPattern:1-362_0_10446_1"));
    }

}
