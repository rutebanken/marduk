package no.rutebanken.marduk.geocoder.netex.geojson;

import no.rutebanken.marduk.exceptions.MardukException;
import no.rutebanken.marduk.geocoder.geojson.GeojsonFeatureWrapperFactory;
import no.rutebanken.marduk.geocoder.netex.TopographicPlaceAdapter;
import no.rutebanken.marduk.geocoder.netex.TopographicPlaceMapper;
import no.rutebanken.marduk.geocoder.netex.TopographicPlaceReader;
import org.apache.commons.io.FileUtils;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.rutebanken.netex.model.MultilingualString;
import org.rutebanken.netex.model.TopographicPlace;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * For reading individual features from geojson files.
 */
public class GeoJsonSingleTopographicPlaceReader implements TopographicPlaceReader {

    private File[] files;

    private static final String LANGUAGE = "en";

    private static final String PARTICIPANT_REF = "WOF";

    private GeojsonFeatureWrapperFactory wrapperFactory;

    public GeoJsonSingleTopographicPlaceReader(GeojsonFeatureWrapperFactory wrapperFactory, File... files) {
        this.files = files;
        this.wrapperFactory = wrapperFactory;
    }


    public List<TopographicPlaceAdapter> read() {
        List<TopographicPlaceAdapter> adapters = new ArrayList<>();
        try {
            for (File file : files) {
                FeatureJSON fJson = new FeatureJSON();
                InputStream inputStream = FileUtils.openInputStream(file);
                SimpleFeature simpleFeature = fJson.readFeature(inputStream);

                TopographicPlaceAdapter adapter = wrapperFactory.createWrapper(simpleFeature);
                if (adapter != null) {
                    adapters.add(adapter);
                }
                inputStream.close();
            }

        } catch (IOException ioE) {
            throw new MardukException("Failed to parse geojson file: " + ioE.getMessage(), ioE);
        }
        return adapters;
    }


    public void addToQueue(BlockingQueue<TopographicPlace> queue) throws IOException, InterruptedException {
        for (File file : files) {
            FeatureJSON fJson = new FeatureJSON();
            InputStream inputStream = FileUtils.openInputStream(file);
            SimpleFeature simpleFeature = fJson.readFeature(inputStream);

            TopographicPlaceAdapter adapter = wrapperFactory.createWrapper(simpleFeature);
            TopographicPlace topographicPlace = new TopographicPlaceMapper(adapter, PARTICIPANT_REF).toTopographicPlace();

            if (topographicPlace != null) {
                queue.put(topographicPlace);
            }
            inputStream.close();
        }
    }

    @Override
    public String getParticipantRef() {
        return PARTICIPANT_REF;
    }

    @Override
    public MultilingualString getDescription() {
        return new MultilingualString().withLang(LANGUAGE).withValue("Whosonfirst neighbouring countries");
    }
}