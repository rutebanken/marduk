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

package no.rutebanken.marduk.gtfs;

import no.rutebanken.marduk.Constants;
import no.rutebanken.marduk.routes.google.GoogleRouteTypeCode;
import org.apache.camel.Header;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ShapePoint;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.services.GtfsMutableRelationalDao;
import org.onebusaway.gtfs_transformer.GtfsTransformer;
import org.onebusaway.gtfs_transformer.factory.EntitiesTransformStrategy;
import org.onebusaway.gtfs_transformer.match.AlwaysMatch;
import org.onebusaway.gtfs_transformer.match.TypedEntityMatch;
import org.onebusaway.gtfs_transformer.services.EntityTransformStrategy;
import org.onebusaway.gtfs_transformer.services.GtfsTransformStrategy;
import org.onebusaway.gtfs_transformer.services.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;


/**
 * For transforming GTFS files.
 */
@Service
public class GtfsTransformationService {


    private static Logger logger = LoggerFactory.getLogger(GtfsTransformationService.class);


    /**
     * Google does not support (all) values in the Extended Route Types code set.
     * <p>
     * GTFS to Google needs to be "dumbed" down to the google supported code set first.
     * <p>
     * * @param includeShapes whether shape data from input file should be included in transformed output
     */
    public File transformToGoogleFormat(File inputFile, @Header(value = Constants.INCLUDE_SHAPES) Boolean includeShapes)  {
        long t1 = System.currentTimeMillis();
        boolean removeShapes = !Boolean.TRUE.equals(includeShapes);

        File transformedGtfsFile = new GoogleGtfsFileTransformer(removeShapes).transform(inputFile);

        logger.debug("Replaced Extended Route Types with google supported values in GTFS-file {} - spent {} ms", inputFile.getName(),  (System.currentTimeMillis() - t1));

        return transformedGtfsFile;
    }

    /**
     * Entur gtfs contains fields and values proposed as extensions to the GTFS standard.
     *
     * @param includeShapes whether shape data from input file should be included in transformed output
     */
    public File transformToBasicGTFSFormat(File inputFile, @Header(value = Constants.INCLUDE_SHAPES) Boolean includeShapes) {
        long t1 = System.currentTimeMillis();

        boolean removeShapes = !Boolean.TRUE.equals(includeShapes);
        File transformedGtfsFile  = new BasicGtfsFileTransformer(removeShapes).transform(inputFile);

        logger.debug("Replaced Extended Route Types with basic values in GTFS-file {} - spent {} ms", inputFile.getName(), (System.currentTimeMillis() - t1));

        return transformedGtfsFile;
    }


    private class GoogleGtfsFileTransformer extends CustomGtfsFileTransformer {
        private Boolean removeShapes;

        public GoogleGtfsFileTransformer(boolean removeShapes) {
            this.removeShapes = removeShapes;
        }

        @Override
        protected void addCustomTransformations(GtfsTransformer transformer) {

            if (removeShapes) {
                transformer.addTransform(new BasicExtendedRouteTypeTransformer.RemoveShapeTransformer());
                transformer.addTransform(createRemoveTripShapeIdStrategy());
            }
            transformer.addTransform(createEntitiesTransformStrategy(Route.class, new GoogleExtendedRouteTypeTransformer()));
            transformer.addTransform(createEntitiesTransformStrategy(Stop.class, new GoogleExtendedRouteTypeTransformer()));
        }
    }

    private class BasicGtfsFileTransformer extends CustomGtfsFileTransformer {
        private boolean removeShapes;

        public BasicGtfsFileTransformer(boolean removeShapes) {
            this.removeShapes = removeShapes;
        }

        @Override
        protected void addCustomTransformations(GtfsTransformer transformer) {
            if (removeShapes) {
                transformer.addTransform(new BasicExtendedRouteTypeTransformer.RemoveShapeTransformer());
                transformer.addTransform(createRemoveTripShapeIdStrategy());
            }
            transformer.addTransform(createEntitiesTransformStrategy(Route.class, new BasicExtendedRouteTypeTransformer()));
            transformer.addTransform(createEntitiesTransformStrategy(Stop.class, new BasicExtendedRouteTypeTransformer()));
        }
    }

    private EntitiesTransformStrategy createRemoveTripShapeIdStrategy() {
        EntitiesTransformStrategy removeShapeStrategy = new EntitiesTransformStrategy();
        removeShapeStrategy.addModification(new TypedEntityMatch(Trip.class, new AlwaysMatch()), (context, dao, entity) -> ((Trip) entity).setShapeId(null));
        return removeShapeStrategy;
    }

    /**
     * "Dumb" down route type code set used for routes and stop points to something that google supports.
     */
    private static class GoogleExtendedRouteTypeTransformer implements EntityTransformStrategy {
        @Override
        public void run(TransformContext context, GtfsMutableRelationalDao dao, Object entity) {
            if (entity instanceof Route) {
                Route route = (Route) entity;
                route.setType(convertRouteType(route.getType()));
            } else if (entity instanceof Stop) {
                Stop stop = (Stop) entity;
                stop.setVehicleType(convertRouteType(stop.getVehicleType()));
            }
        }

        private int convertRouteType(int extendedType) {
            if (extendedType < 0) {
                return extendedType;
            }

            return GoogleRouteTypeCode.toGoogleSupportedRouteTypeCode(extendedType);
        }
    }

    /**
     * "Dumb" down route type code set used for routes and stop points to the values in the GTFS specification.
     */
    private static class BasicExtendedRouteTypeTransformer implements EntityTransformStrategy {


        @Override
        public void run(TransformContext context, GtfsMutableRelationalDao dao, Object entity) {
            if (entity instanceof Route) {
                Route route = (Route) entity;
                route.setType(BasicRouteTypeCode.convertRouteType(route.getType()));
            } else if (entity instanceof Stop) {
                Stop stop = (Stop) entity;
                stop.setVehicleType(BasicRouteTypeCode.convertRouteType(stop.getVehicleType()));
            }
        }



        private static class RemoveShapeTransformer implements GtfsTransformStrategy {

            @Override
            public void run(TransformContext context, GtfsMutableRelationalDao dao) {
                dao.clearAllEntitiesForType(ShapePoint.class);
            }

        }
    }

    public static EntitiesTransformStrategy createEntitiesTransformStrategy(Class<?> entityClass, EntityTransformStrategy strategy) {
        EntitiesTransformStrategy transformStrategy = new EntitiesTransformStrategy();
        transformStrategy.addModification(new TypedEntityMatch(entityClass, new AlwaysMatch()), strategy);
        return transformStrategy;
    }

}
