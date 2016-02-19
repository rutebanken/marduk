docker rm -f marduk ; mvn -Pf8-build && docker run -it --name marduk -e JAVA_OPTIONS="-Xmx4g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" --link activemq --link lamassu --link chouette -p 5005:5005 -v ~/.ssh/lamassu.pem:/opt/jboss/.ssh/lamassu.pem:ro -v /git/config/marduk/dev/application.properties:/app/config/application.properties:ro dr.rutebanken.org/rutebanken/marduk:0.0.1-SNAPSHOT
