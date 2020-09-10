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

package no.rutebanken.marduk.routes.chouette.json.exporter;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

public class DateUtils {

    private DateUtils() {
    }

    public static Date startDateFor(long daysBack){
        return Date.from(LocalDate.now().atStartOfDay().minusDays(daysBack).atZone(ZoneId.systemDefault()).toInstant());
    }

    public static Date endDateFor(long daysForward){
        return Date.from(LocalDate.now().atStartOfDay().plusDays(daysForward).atZone(ZoneId.systemDefault()).toInstant());
    }

}
