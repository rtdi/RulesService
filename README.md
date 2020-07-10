# RTDI Rules Service

_Validate incoming messages and augment them with rule testing results_

Source code available here: [github](https://github.com/rtdi/RTDIRulesService)

## Design Thinking goal
* As a business user I would like to validate the incoming data and cleanse it in realtime
* Consumers have the choice to read the raw or the cleansed data
* Operational dashboards using the rule results provide information about the data quality
* Different types of rules should be supported, validation rules, cleansing rules, data augmentation, standardisation rules,...


## Installation and testing

On any computer install the Docker Daemon - if it is not already - and download this docker image with

    docker pull rtdi/rulesservice

Then start the image via docker run. For a quick test this command is sufficient

    docker run -d -p 80:8080 --rm --name rulesservice  rtdi/rulesservice

to expose a webserver at port 80 on the host running the container. Make sure to open the web page via the http prefix, as https needs more configuration.
For example [http://localhost:80/](http://localhost:80/) might do the trick of the container is hosted on the same computer.

The default login for this startup method is: **rtdi / rtdi!io**

The probably better start command is to mount two host directories into the container. In this example the host's /data/files contains all files to be loaded into Kafka and the /data/config is an (initially) empty directory where all settings made when configuring the connector will be stored permanently.

    docker run -d -p 80:8080 --rm -v /data/files:/data/ -v /data/config:/usr/local/tomcat/conf/security \
       --name rulesservice  rtdi/rulesservice


For proper start commands, especially https and security related, see the [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) project, this application is based on.

  

### Connect the Pipeline to Kafka

The first step is to connect the application to a Kafka server, in this example Confluent Cloud.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-PipelineConfig.png" width="50%">


### Define Services

Each Service is a Kakfa KStream, a distributed process listening on a topic and validating the data. Hence the first setting of each service are the input and output topic names to use.

Within one rule service multiple steps can be performed, a microservice transformation so to speak. These transformation steps happen within the service. For example the first microservice step might standardize on the different spellings of the field STATUS. The result of this step is then put into another step, validating if the STATUS is valid and consistent with other data.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleDefinition-Level1.png" width="50%">

As one topic can contain data with different schemas, the next selection is the schema the ruleset is defined for. As the rules are different for sales order header, order line item, customer etc, each ruleset is applied to data of the particular type only. If a record schema has no rules assigned, then it is passed through as is.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleDefinition-Level2.png" width="50%">

The final step are the rules themselves. The structure of the schema with all its nesting elements are shown and rules can be defined for each field. Either a single "Test a column value" type of rule or multiple rules. The latter makes sense to make the rule formula more simple by breaking it into multiple individual ones until all tests have been made or until the first rule failed.

An example would be a rule on the STATUS column, which must contain the values Ordered, Confirmed, InProduction or Completed. One way would be a a single rule on the column with a OR condition. The other option would be five rules testing for a single status value only and the group is "Test until first Passes". 

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleDefinition-Level3.png" width="50%">



### Data content

The result of all rule transformations is stored in the audit array of the record. Querying this data allows detailed reporting which records were processed by what rule and the results.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleResult.png" width="50%">


## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.