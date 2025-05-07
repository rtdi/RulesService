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
import org.glassfish.jersey.server.ChunkedOutput;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.rtdi.bigdata.rulesservice.LoggingUtil;
import io.rtdi.bigdata.rulesservice.RulesService;
import io.rtdi.bigdata.rulesservice.config.SampleFileName;
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
public class RestServiceSample {
	protected static final int SAMPLE_MAX_ROWS = 100;

	@Context
	private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@Context
	HttpServletRequest request;


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
			LoggingUtil.logRequestBegin(request);
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
								LoggingUtil.logRequestEnd(request);
							} catch (IOException | RestClientException e) {
								LoggingUtil.logRequestEndInputError(request, e);
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
			LoggingUtil.logRequestEndInputError(request, e);
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
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			String filename = data.save(service.getRuleFileRootDir());
			return LoggingUtil.requestEnd(request, new SampleFileName(filename));
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
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
			LoggingUtil.logRequestBegin(request);
			RulesService service = RulesService.getRulesService(servletContext);
			List<SampleFileName> samplefiles = SampleData.getFiles(service.getRuleFileRootDir(), subject);
			return LoggingUtil.requestEnd(request, samplefiles);
		} catch (Exception e) {
			return LoggingUtil.requestEndTechnicalError(request, e);
		}
	}

}
