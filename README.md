# RTDI Rules Service

_Validate incoming messages and augment them with rule testing results_

Source code available here: [github](https://github.com/rtdi/RTDIRulesService)
Docker image here: [dockerhub](https://hub.docker.com/r/rtdi/rulesservice)

## Design Thinking goal

* As a business user I would like to validate the incoming data and cleanse it in realtime
* Consumers have the choice to read the raw or the cleansed data
* Operational dashboards using the rule results provide information about the data quality
* Different types of rules should be supported, validation rules, cleansing rules, data augmentation, standardization rules,...

## Requirements

* Payload (value) in Avro Format
* Apache Kafka with the permissions to add a KStream
* Schema Registry


## Installation and testing

On any computer install the Docker Daemon - if not already done - and download the docker image with

    docker pull rtdi/rulesservice

Then start the image via docker run. For a quick test this command is sufficient

    docker run -d -p 80:8080 --rm --name rulesservice  rtdi/rulesservice

to expose a webserver at port 80 on the host running the container. Make sure to open the web page via the http prefix, as https needs more configuration.
For example [http://localhost:80/](http://localhost:80/) might do the trick if the container is hosted on the same computer.

The default login for this startup method is: **rtdi / rtdi!io**

The probably better start command is to mount two host directories into the container, the rtdiconfig directory where all settings made when configuring the connector will be stored permanently and the security directory for web server specific settings like user database and SSL certificates.

    docker run -d -p 80:8080 -p 443:8443 --rm -v /data/files:/data/ \
       -v /home/dir/rtdiconfig:/usr/local/tomcat/conf/rtdiconfig \
       -v /home/dir/security:/usr/local/tomcat/conf/security \
        --name rulesservice  rtdi/rulesservice



For proper start commands, especially https and security related, see the [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) project, this application is based on.

  

### Connect the Pipeline to Kafka

The first step is to connect the application to a Kafka server and the schema registry.


### Define Services

The rules are defined by subject. To help with the rule formulas, sample data can be gathered from the topics and used when defining the rules.
Once a rule file is complete, it must be activated and associated with an input topic.

### Result

The input schema is copied as new schema, which has an additional `_audit` field, containing all audit results.
There the overall rule result is stored (did it pass all tests?), a list of all rules executed and their individual results.
Querying this data allows detailed reporting which records were processed by what rule and the results.

<img src="https://github.com/rtdi/RulesService/raw/master/docs/media/RulesService-RuleResult.png" width="50%">

## Architecture

This service is runs one KStream application per topic, reading the input topic, applying the rules and sending the result to the defined output topic.
While the key can be of any format, the value must be an Avro record.

## Rules

### Rule types

Imagine the input has a field `NAME` which can be null but then we want to replace it with a `?`. Such a null value is not wrong as such but we do want to see it as a warning.

Translated into the UI, we add a rule to the field and as rule condition to be met the formula `NAME != null`. If that condition returns true, the name has a value other than null, then the rule result is `=pass`. Else its rule result is whatever is set in the UI as `if test failed...`, in our case `=warn`.
The `"?"` is entered in the `...change value to`, to assign that text to the field if the condition is not met.

The UI also shows the node `(more)`, which is the place to enter all rules that do not belong to a single field. Such generic rules cannot have a replacement value for obvious reasons.

No matter if a field rule or a generic rule is entered, the formulas have access to all fields of the message. For example, a rule for the input field `NAME` might either use the formula `NAME != null` or `NAME != null && COMPANY_NAME != null`. A field level rule is not limited to formulas using this field only!

In case of a nested structure as input, rules can specified at all levels and are executed for each record in that level. 
For example, the `CUSTOMER` schema might have an array of `ADDRESSES`. If a rule is added at the root level, it will be executed only once. If it is inside the `ADDRESSES` array, it will be executed once per address - for each address record in the array.

With above capabilities all rules can be defined. The more conditions the more complex the formulas get, which is not good.
For that reason there are more rule types, the Test-Set rule to specify multiple individual rules

- Test for all conditions: Every single rule is applied, thus acting as a AND combination of rules. This is convenient for cases where a field needs to be checked for different conditions, e.g. the OrderDate has to be within three months or raise a warning and the OrderDate cannot be later than the expected delivery date. The rule set will return the lowest individual rule result. If all passed the result is pass. If at least one said warning, the result is a warning. And if one is failed, the result is failed.
- Test until one failed: Each rule will be tested and stopped once the first rule violation is found. It is a AND combination of the rules as well but while in above case all rules are tested, here all other rules are not added to the test result. The rule set will return failed when at least one is failed and pass only if all rules passed.
- Test until one passes: This is an OR condition. If condition1 is not met, maybe condition2 is. As soon as the first positive test is found, no further processing is needed, a valid value has been found. The rule set will return failed only if all tests failed. One positive test makes the entire rule set positive.

For each individual rule the rule result can be specified if the condition is met. This way the rule can specify the severity, e.g. a SoldTo == null shall be treated as failed, SoldTo.length() < 5 as well but SoldTo.length() < 10 shall be a warning only.
Other tests might not impact the rule result at all, they return passed always. For those the audit array will show that the rule has been tested but the data is of no harm. For example in a gender column the test could be if the value is either M or F and in all other cases a substitution value of X is used. As the gender does return the values M,F or X only, it is to be considered valid.

A more extreme case would be to assign a column with a fixed value. In that case the condition is the formula "true", the rules result will be pass and the substitution formula the constant to assign.

### Rule syntax

When entering formulas, the columns of the current level can be used directly. For example on the SalesItems level a formula might be "MaterialNumber != null" if the SalesItems structure consists of records that have a MaterialNumber column. The keyword "parent" refers to the parent element, in this example the array of SalesItems.
Thus a formula might be "LineNumber <= parent.length()" if the assumption is, 3 line items are numbered as items 1,2 and 3.

Note: The library used here is [Apache JEXL](https://commons.apache.org/proper/commons-jexl/reference/syntax.html).

## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.