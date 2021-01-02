FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/RulesService/master/docker/global.properties conf/rtdiconfig/RulesService/
ADD https://github.com/rtdi/RulesService/releases/latest/download/rulesservice.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
