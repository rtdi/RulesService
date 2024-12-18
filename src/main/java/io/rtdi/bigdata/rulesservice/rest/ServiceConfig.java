package io.rtdi.bigdata.rulesservice.rest;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.jersey.server.ChunkedOutput;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.RuleFileName;
import io.rtdi.bigdata.rulesservice.config.SampleFileName;
import io.rtdi.bigdata.rulesservice.config.ServiceSettings;
import io.rtdi.bigdata.rulesservice.config.SubjectName;
import io.rtdi.bigdata.rulesservice.config.TopicName;
import io.rtdi.bigdata.rulesservice.config.TopicRule;
import io.rtdi.bigdata.rulesservice.definition.RuleFileDefinition;
import io.rtdi.bigdata.rulesservice.definition.RuleStep;
import io.rtdi.bigdata.rulesservice.rules.RuleUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Path("/")
public class ServiceConfig {
	protected static final int SAMPLE_MAX_ROWS = 100;

	protected final Logger log = LogManager.getLogger(this.getClass().getName());

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;

	@GET
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Return the service status",
			description = "Returns the status of the rules service",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Configuration data",
							content = {
									@Content(
											schema = @Schema(implementation = ServiceConfig.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getConfig() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			boolean admin = request.isUserInRole(ServletSecurityConstants.ROLE_ADMIN);
			ServiceSettings ret = service.getConfig(admin);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_ADMIN)
	@Operation(
			summary = "Save the config",
			description = "save the config file and re-establish all connections",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											schema = @Schema(implementation = SuccessResponse.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response postConfig(@RequestBody ServiceSettings input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			service.saveConfig(input);
			return LoggingUtil.requestEnd(log, request, new SuccessResponse("Saved"));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All subjects",
			description = "Returns a list of all subjects from the schema registry",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of subjects",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = SubjectName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getSubjects() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<String> subjects = service.getSubjects();
			List<SubjectName> ret = new ArrayList<>();
			for (String s : subjects) {
				ret.add(new SubjectName(s));
			}
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects/{subject}/rules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All rules for a subject",
			description = "Returns a list of all rule groups using that subject is using as input from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = RuleFileName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getRuleFiles(
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<RuleFileName> rulegroups = service.getRuleFiles(subjectname);
			return LoggingUtil.requestEnd(log, request, rulegroups);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/rules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "All rules",
			description = "Returns a list of all active rule groups from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = RuleFileName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getAllRuleFiles() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<RuleFileName> rulegroups = service.getAllRuleGroups();
			return LoggingUtil.requestEnd(log, request, rulegroups);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Get a rule group definition",
			description = "Returns the rule group definition from the file system",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											schema = @Schema(implementation = RuleFileDefinition.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			java.nio.file.Path name = java.nio.file.Path.of(path);
			RuleFileDefinition rg = RuleFileDefinition.load(service.getRuleFileRootDir(), subjectname, name, false);
			return LoggingUtil.requestEnd(log, request, rg);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Save a rule file definition",
			description = "Saves the posted rule file, a pure file system operation",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											schema = @Schema(implementation = SuccessResponse.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response postRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file to save; note that this can be different from the name inside the payload",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname,
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			java.nio.file.Path name = java.nio.file.Path.of(path); // The name of the file to be saved, which is the old name
			input.setActive(false);
			input.setInputsubjectname(subjectname);
			input.save(service.getRuleFileRootDir(), name);
			return LoggingUtil.requestEnd(log, request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@PATCH
	@Path("/subjects/{subject}/rules/{path:.*}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Active a rule file",
			description = "Returns a list of all rule groups using that subject is using as input, a pure file system operation",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											schema = @Schema(implementation = SuccessResponse.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response activateRuleFile(
			@PathParam("path")
			@Parameter(
					description = "name of the rule group file to save; note that this can be different from the name inside the payload",
					example = "ruleset1/address_rules.json"
					)
			String path,
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition.copyToActivate(service.getRuleFileRootDir(), subjectname, path);
			return LoggingUtil.requestEnd(log, request, SuccessResponse.SUCCESS);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/subjects/{subject}/defaultrule")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Create an empty rule group definition",
			description = "Create a new rule group for that subject",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "List of all rule group names",
							content = {
									@Content(
											schema = @Schema(implementation = RuleFileDefinition.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response createEmptyRuleFile(
			@PathParam("subject")
			@Parameter(
					description = "name of subject",
					example = "order_data-value"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			org.apache.avro.Schema schema = service.getLatestSchema(subjectname);
			RuleFileDefinition rg = new RuleFileDefinition(schema);
			rg.setInputsubjectname(subjectname);
			RuleStep step = new RuleStep("next step", schema);
			rg.addRuleStep(step);
			RuleUtils.addRules(step, schema);
			return LoggingUtil.requestEnd(log, request, rg);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}


	@GET
	@Path("/subjects/{subject}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "Get the schema definition",
			description = "Returns the schema definition from the schema registry",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "schema metadata",
							content = {
									@Content(
											schema = @Schema(implementation = Schema.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getSchemaDefiniton(
			@PathParam("subject")
			@Parameter(
					description = "name of value subject",
					example = "order_data"
					)
			String subjectname) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			org.apache.avro.Schema schema = service.getLatestSchema(subjectname);
			return LoggingUtil.requestEnd(log, request, schema);
		} catch (RestClientException e) {
			if (e.getStatus() == 404) {
				// invalid schema
				return LoggingUtil.requestEndInputError(log, request, e, Status.NOT_FOUND);
			} else {
				return LoggingUtil.requestEndTechnicalError(log, request, e);
			}
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/topicrules")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "List of topics and their rules",
			description = "Returns a list of all topics and the rules assigned, if there are any",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "rules",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicRule.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getTopicRules() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<TopicRule> topicrules = service.getTopicRules();
			return LoggingUtil.requestEnd(log, request, topicrules);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/topicrules")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Save the modified rules",
			description = "Go through the input object and save all rules with a modified=true flag",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicRule.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response postRules(@RequestBody List<TopicRule> input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<TopicRule> ret = service.saveTopicRules(input);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/topics")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "List of topics",
			description = "Returns a list of all topics",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "rules",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = TopicName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getTopics() {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			Collection<String> topics = service.getTopics();
			List<TopicName> topicnames = new ArrayList<>(topics.size());
			for (String t : topics) {
				topicnames.add(new TopicName(t));
			}
			return LoggingUtil.requestEnd(log, request, topicnames);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/sample")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Retrieve sample data",
			description = "Query the list of provided topics",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Sample payloads",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = SampleData.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response sample(@RequestBody List<String> topics) {
		ObjectMapper om = new ObjectMapper();
		try {
			LoggingUtil.logRequestBegin(log, request);
			ChunkedOutput<String> output = new ChunkedOutput<String>(String.class);
			if (topics == null || topics.size() == 0) {
				return ErrorResponse.createErrorResponse(new IOException("No topics to read from have been provided"), Status.BAD_REQUEST);
			} else {
				RulesService service = RulesService.getRulesService(servletContext);

				new Thread() {

					@Override
					public void run() {
						Properties consumerProperties = new Properties();
						consumerProperties.putAll(service.getKafkaProperties());
						consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
						consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
						consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
						consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
						consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // not actually used

						try (KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(consumerProperties);) { // closing the consumer takes a while, close the output first hence
							try {
								/*
								 * get the latest offsets for each partition
								 */
								Collection<TopicPartition> partitions = new ArrayList<>();
								for (String topic : topics) {
									List<PartitionInfo> partitioninfo = consumer.partitionsFor(topic, Duration.ofSeconds(20));
									for (PartitionInfo partition : partitioninfo) {
										TopicPartition t = new TopicPartition(partition.topic(), partition.partition());
										partitions.add(t);
									}
								}
								Map<TopicPartition, Long> endoffsets = consumer.endOffsets(partitions);
								/*
								 * Assign all partitions
								 */
								consumer.assign(partitions);
								/*
								 * Configure the consumer offset to the current offset, which is the offset of the next future record, minus 10 to
								 * read a few recent record, hopefully one with the proper subject
								 */
								for (Entry<TopicPartition, Long> partition : endoffsets.entrySet()) {
									long endoffset = partition.getValue() >= 10 ? partition.getValue()-10L : 0L;
									consumer.seek(partition.getKey(), endoffset);
								}
								long endtime = System.currentTimeMillis() + 20000L;
								int rows = 0;
								while (System.currentTimeMillis() < endtime && rows < SAMPLE_MAX_ROWS && endoffsets.size() > 0) {
									ConsumerRecords<Bytes, Bytes> records = consumer.poll(Duration.ofSeconds(2));

									Iterator<ConsumerRecord<Bytes, Bytes>> iter = records.iterator();
									while (iter.hasNext() && rows < SAMPLE_MAX_ROWS && endoffsets.size() > 0) {
										ConsumerRecord<Bytes, Bytes> recordset = iter.next();
										SampleData sampledata = new SampleData(recordset, service.getSchemaclient());
										String json = om.writeValueAsString(sampledata);
										output.write(json + "\n");
										rows++;
										/*
										 * If the endoffset was reached for a topic partition, then remove it so we can end early
										 */
										TopicPartition p = new TopicPartition(recordset.topic(), recordset.partition());
										Long endoffset = endoffsets.get(p);
										if (endoffset != null && recordset.offset() >= endoffset-1) {
											endoffsets.remove(p);
										}
									}
								}
								LoggingUtil.logRequestEnd(log, request);
							} catch (IOException | RestClientException e) {
								LoggingUtil.logRequestEndInputError(log, request, e);
								ErrorResponse response = new ErrorResponse(e);
								try {
									output.write(om.writeValueAsString(response));
								} catch (IOException e1) {
								}
							} finally {
								try {
									output.close();
								} catch (IOException e) {
								}
							}
						}
					}
				}.start();
				return Response.ok(output).build();
			}
		} catch (Exception e) {
			LoggingUtil.logRequestEndInputError(log, request, e);
			return ErrorResponse.createErrorResponse(e);
		}
	}

	@POST
	@Path("/sample/{subject}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Save one sample record",
			description = "Saves the sample record as file",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											schema = @Schema(implementation = SampleFileName.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response savesample(
			@PathParam("subject")
			@Parameter(
					description = "subject name ",
					example = "order-value"
					)
			String subject,
			@RequestBody SampleData data) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			String filename = data.save(service.getRuleFileRootDir());
			return LoggingUtil.requestEnd(log, request, new SampleFileName(filename));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@GET
	@Path("/sample/{subject}")
	@Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	@Operation(
			summary = "List of all sample data",
			description = "Returns a list of all sample data files for the subject",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "sample files",
							content = {
									@Content(
											array = @ArraySchema(schema = @Schema(implementation = SampleFileName.class))
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response getSampleFiles(
			@PathParam("subject")
			@Parameter(
					description = "subject name ",
					example = "order-value"
					)
			String subject
			) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<SampleFileName> samplefiles = SampleData.getFiles(service.getRuleFileRootDir(), subject);
			return LoggingUtil.requestEnd(log, request, samplefiles);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/calculate/{stepindex}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "apply the provided sample data for this step",
			description = "post the entire rule file but process just a single step using its sample value",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "Success message",
							content = {
									@Content(
											schema = @Schema(implementation = RuleFileDefinition.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response processStep(
			@PathParam("stepindex")
			@Parameter(
					description = "index of the step to process",
					example = "0"
					)
			Integer stepindex,
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleStep ret = service.applySampleData(input, stepindex);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

	@POST
	@Path("/calculate")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_EDIT)
	@Operation(
			summary = "Apply the selected sample file to the rule file",
			description = "Calculate all rules using the specified rule file",
			responses = {
					@ApiResponse(
							responseCode = "200",
							description = "The rule file with the new sample values",
							content = {
									@Content(
											schema = @Schema(implementation = RuleFileDefinition.class)
											)
							}
							),
					@ApiResponse(
							responseCode = "500",
							description = "Any exception thrown",
							content = {
									@Content(
											schema = @Schema(implementation = ErrorResponse.class)
											)
							}
							)
			})
	public Response processAll(
			@RequestBody RuleFileDefinition input) {
		try {
			LoggingUtil.logRequestBegin(log, request);
			RulesService service = RulesService.getRulesService(servletContext);
			RuleFileDefinition ret = service.applySampleFile(input);
			return LoggingUtil.requestEnd(log, request, ret);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(log, request, e);
		}
	}

}
