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

Within one rule service for each schema multiple steps can be performed, a microservice transformation so to speak. These transformation steps happen within the service. For example the first microservice step might standardize on the different spellings of the field STATUS. The result of this step is then put into another step, validating if the STATUS is valid and consistent with other data. In the screen schemas are added and then the step names - they are executed in named order.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleDefinition-Level1.png" width="50%">

Once the structure is saved, by clicking on the individual steps the rules themselves are defined. The structure of the schema with all its nesting elements are shown and rules can be defined for each field.

An example would be a rule on the STATUS column, which must contain the values Ordered, Confirmed, InProduction or Completed. One way would be a a single rule on the column with a OR condition. The other option would be five rules testing for a single status value only and the group is "Test until first Passes". 

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleDefinition-Level3.png" width="50%">

### Data content

The result of all rule transformations is stored in the audit array of the record. Querying this data allows detailed reporting which records were processed by what rule and the results.
If the input schema does not have such __audit array, a new version of the schema will be created automatically.

<img src="https://github.com/rtdi/RTDIRulesService/raw/master/docs/media/RulesService-RuleResult.png" width="50%">

### Rule types

Rules can be applied on fields only and this field acts as the trigger and is used when providing a substitution value.
For example, the SalesOrderHeader has a field SoldTo at root level, hence all rules will be executed once per message.

A rule on a field of the SalesItems array will be triggered for each item. Thus the field a rule belongs to controls primarily that.

For each field either a single rule "Test a column" can be defined or a Test-Set with multiple rules.

- Test for all conditions: Every single rule is applied, thus acting as a AND combination of rules. This is convenient for cases where a field needs to be checked for different conditions, e.g. the OrderDate has to be within three months or raise a warning and the OrderDate cannot be later than the expected delivery date. The rule set will return the lowest individual rule result. If all passed the result is pass. If at least one said warning, the result is a warning. And if one is failed, the result is failed.
- Test until one failed: Each rule will be tested and stopped once the first rule violation is found. It is a AND combination of the rules as well but while in above case all rules are tested, here all other rules are not added to the test result. The rule set will return failed when at least one is failed and pass only if all rules passed.
- Test until one passes: This is an OR condition. If condition1 is not met, maybe condition2 is. As soon as the first positive test is found, no further processing is needed, a valid value has been found. The rule set will return failed only if all tests failed. One positive test makes the entire rule set positive.

For each individual rule the rule result can be specified if the condition is met. This way the rule can specify the severity, e.g. a SoldTo == null shall be treated as failed, SoldTo.length() < 5 as well but SoldTo.length() < 10 shall be a warning only.
Other tests might not impact the rule result at all, they return passed always. For those the audit array will show that the rule has been tested but the data is of no harm. For example in a gender column the test could be if the value is either M or F and in all other cases a substitution value of X is used. As the gender does return the values M,F or X only, it is to be considered valid.

A more extreme case would be to assign a column with a fixed value. In that case the condition is the forumla "true", the rules result will be pass and the substitution formula the constant to assign.

### Rule syntax

When entering formulas, the columns of the current level can be used directly. For example on the SalesItems level a formula might be "MaterialNumber != null" if the SalesItems structure consists of records that have a MaterialNumber column. The keyword "parent" refers to the parent element, in this example the array of SalesItems.
Thus a formula might be "LineNumber <= parent.length()" if the assumption is, 3 line items are numbered as items 1,2 and 3.

Note: The library used here is [Apache JEXL](https://commons.apache.org/proper/commons-jexl/reference/syntax.html).

## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.