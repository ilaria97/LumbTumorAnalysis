FROM docker.elastic.co/logstash/logstash:8.1.1
RUN rm -f /usr/share/logstash/pipeline/logstash.conf
ADD pipeline/ /usr/share/logstash/pipeline/
ADD csv/ /usr/share/logstash/csv/

#NO! Questa immagine di logstash non prende in considerazione il Dockerfile
# and run with 
# docker run --rm --network tap -it -v $PWD/pipeline/:/usr/share/logstash/pipeline/ docker.elastic.co/logstash/logstash:8.0.0 
# make sure that pipeline dir with proper conf is there


#esegui ../bin/ ./logstash.sh