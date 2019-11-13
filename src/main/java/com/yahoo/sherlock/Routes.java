/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.yahoo.egads.data.Anomaly;
import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.exception.ClusterNotFoundException;
import com.yahoo.sherlock.exception.DruidException;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.DruidCluster;
import com.yahoo.sherlock.model.EgadsResult;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.model.JobTimeline;
import com.yahoo.sherlock.model.JsonTimeline;
import com.yahoo.sherlock.model.UserQuery;
import com.yahoo.sherlock.query.EgadsConfig;
import com.yahoo.sherlock.query.Query;
import com.yahoo.sherlock.query.QueryBuilder;
import com.yahoo.sherlock.service.JobExecutionService;
import com.yahoo.sherlock.service.SchedulerService;
import com.yahoo.sherlock.service.DetectorService;
import com.yahoo.sherlock.service.DruidQueryService;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.service.ServiceFactory;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.DruidClusterAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JsonDumper;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.BackupUtils;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import com.yahoo.sherlock.utils.Utils;

import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.TemplateEngine;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Routes logic for web requests.
 */
@Slf4j
public class Routes {

    /**
     * Default parameters.
     */
    public static Map<String, Object> defaultParams;

    /**
     * Class Thymeleaf template engine instance.
     */
    private static TemplateEngine thymeleaf;

    private static ServiceFactory serviceFactory;
    private static SchedulerService schedulerService;
    private static AnomalyReportAccessor reportAccessor;
    private static DruidClusterAccessor clusterAccessor;
    private static JobMetadataAccessor jobAccessor;
    private static DeletedJobMetadataAccessor deletedJobAccessor;
    private static EmailMetadataAccessor emailMetadataAccessor;
    private static JsonDumper jsonDumper;

    /**
     * Initialize the default Route parameters.
     */
    public static void initParams() {
        defaultParams = new HashMap<>();
        defaultParams.put(Constants.PROJECT, CLISettings.PROJECT_NAME);
        defaultParams.put(Constants.TITLE, Constants.SHERLOCK);
        defaultParams.put(Constants.VERSION, CLISettings.VERSION);
        defaultParams.put(Constants.ERROR, null);
        defaultParams.put(Constants.INSTANTVIEW, null);
        defaultParams.put(Constants.DELETEDJOBSVIEW, null);
        defaultParams.put(Constants.GRANULARITIES, Granularity.getAllValues());
        List<String> frequencies = Triggers.getAllValues();
        frequencies.remove(Triggers.INSTANT.toString());
        defaultParams.put(Constants.FREQUENCIES, frequencies);
        defaultParams.put(Constants.EMAIL_HTML, null);
        defaultParams.put(Constants.EMAIL_ERROR, null);
    }

    /**
     * Initialize and set references to service and
     * accessor objects.
     */
    public static void initServices() {
        thymeleaf = new ThymeleafTemplateEngine();
        serviceFactory = new ServiceFactory();
        schedulerService = serviceFactory.newSchedulerServiceInstance();
        // Grab references to the accessors, initializing them
        reportAccessor = Store.getAnomalyReportAccessor();
        clusterAccessor = Store.getDruidClusterAccessor();
        jobAccessor = Store.getJobMetadataAccessor();
        deletedJobAccessor = Store.getDeletedJobMetadataAccessor();
        emailMetadataAccessor = Store.getEmailMetadataAccessor();
        jsonDumper = Store.getJsonDumper();
        schedulerService.instantiateMasterScheduler();
        schedulerService.startMasterScheduler();
        schedulerService.startEmailSenderScheduler();
        schedulerService.startBackupScheduler();
    }

    /**
     * Inititialize the routes default settings.
     *
     * @throws SherlockException exception in intialization
     */
    public static void init() throws SherlockException {
        initParams();
        initServices();
    }

    /**
     * Method called on root endpoint.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return template view route to home page
     */
    public static ModelAndView viewHomePage(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "homePage");
    }

    /**
     * Method called upon flash query request from user.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewInstantAnomalyJobForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set instant form view
        params.put(Constants.INSTANTVIEW, "true");
        params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
        params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
        params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
        params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
        params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
        params.put(Constants.MINUTE, Constants.MAX_MINUTE);
        params.put(Constants.HOUR, Constants.MAX_HOUR);
        params.put(Constants.DAY, Constants.MAX_DAY);
        params.put(Constants.WEEK, Constants.MAX_WEEK);
        params.put(Constants.MONTH, Constants.MAX_MONTH);
        params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
        params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
        try {
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.DRUID_CLUSTERS, new ArrayList<>());
        }
        return new ModelAndView(params, "jobForm");
    }

    /**
     * Method to display status.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewStatus(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set status
        return new ModelAndView(params, "status");
    }

    /**
     * Method called upon new job form request from user.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return template view route
     */
    public static ModelAndView viewNewAnomalyJobForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Add Job");
        try {
            params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
            params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
            params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
            params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
            params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
            params.put(Constants.MINUTE, Constants.MAX_MINUTE);
            params.put(Constants.HOUR, Constants.MAX_HOUR);
            params.put(Constants.DAY, Constants.MAX_DAY);
            params.put(Constants.WEEK, Constants.MAX_WEEK);
            params.put(Constants.MONTH, Constants.MAX_MONTH);
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
            params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
            params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "jobForm");
    }

    /**
     * Get the user query and generate anomaly report.
     *
     * @param request  User request
     * @param response Anomaly detector response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static ModelAndView processInstantAnomalyJob(Request request, Response response) {
        log.info("Getting user query from request.");
        Map<String, Object> params = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Anomaly Report");
        try {
            Map<String, String> paramsMap = Utils.queryParamsToStringMap(request.queryMap());
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            // regenerate user query
            Granularity granularity = Granularity.getValue(paramsMap.get("granularity"));
            Integer granularityRange = userQuery.getGranularityRange();
            Integer hoursOfLag = clusterAccessor.getDruidCluster(paramsMap.get("clusterId")).getHoursOfLag();
            Integer intervalEndTime;
            ZonedDateTime endTime = TimeUtils.parseDateTime(userQuery.getQueryEndTimeText());
            if (ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag).toEpochSecond() < endTime.toEpochSecond()) {
                intervalEndTime = granularity.getEndTimeForInterval(endTime.minusHours(hoursOfLag));
            } else {
                intervalEndTime = granularity.getEndTimeForInterval(endTime);
            }
            Query query = serviceFactory.newDruidQueryServiceInstance().build(userQuery.getQuery(), granularity, granularityRange, intervalEndTime, userQuery.getTimeseriesRange());
            JobMetadata job = new JobMetadata(userQuery, query);
            job.setFrequency(granularity.toString());
            job.setEffectiveQueryTime(intervalEndTime);
            // set egads config
            EgadsConfig config;
            config = EgadsConfig.fromProperties(EgadsConfig.fromFile());
            config.setTsModel(userQuery.getTsModels());
            config.setAdModel(userQuery.getAdModels());
            // detect anomalies
            List<EgadsResult> egadsResult = serviceFactory.newDetectorServiceInstance().detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    userQuery.getDetectionWindow(),
                    config
            );
            // results
            List<Anomaly> anomalies = new ArrayList<>();
            List<ImmutablePair<Integer, String>> timeseriesNames = new ArrayList<>();
            int i = 0;
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
                timeseriesNames.add(new ImmutablePair<>(i++, result.getBaseName()));
            }
            List<AnomalyReport> reports = serviceFactory.newJobExecutionService().getReports(anomalies, job);
            tableParams.put(Constants.INSTANTVIEW, "true");
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            params.put("tableHtml", thymeleaf.render(new ModelAndView(tableParams, "table")));
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            params.put("data", new Gson().toJson(EgadsResult.fuseResults(egadsResult), jsonType));
            params.put("timeseriesNames", timeseriesNames);
        } catch (IOException | ClusterNotFoundException | DruidException | SherlockException e) {
            log.error("Error while processing instant job!", e);
            params.put(Constants.ERROR, e.toString());
        } catch (Exception e) {
            log.error("Unexpected error!", e);
            params.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(params, "reportInstant");
    }

    /**
     * Method for saving user anomaly job into database.
     *
     * @param request  HTTP request whose body contains the job info
     * @param response HTTP response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String saveUserJob(Request request, Response response) {
        log.info("Getting user query from request to save it in database.");
        try {
            // Parse user request
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            // Validate user email
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (!validEmail(userQuery.getOwnerEmail(), emailService)) {
                throw new SherlockException("Invalid owner email passed");
            }
            log.info("User request parsing successful.");
            DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
            Query query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), userQuery.getGranularityRange(), null, userQuery.getTimeseriesRange());
            log.info("Query generation successful.");
            // Create and store job metadata
            JobMetadata jobMetadata = new JobMetadata(userQuery, query);
            jobMetadata.setHoursOfLag(clusterAccessor.getDruidCluster(jobMetadata.getClusterId()).getHoursOfLag());
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            return jobMetadata.getJobId().toString(); // return job ID
        } catch (Exception e) {
            response.status(500);
            log.error("Error ocurred while saving job!", e);
            return e.getMessage();
        }
    }

    /**
     * Method for deleting user anomaly job into database.
     *
     * @param request  Request for an anomaly report
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteJob(Request request, Response response) {
        Integer jobId = NumberUtils.parseInt(request.params(Constants.ID));
        log.info("Getting job list from database to delete the job [{}]", jobId);
        try {
            schedulerService.stopJob(jobId);
            jobAccessor.deleteJobMetadata(jobId);
            return Constants.SUCCESS;
        } catch (IOException | JobNotFoundException | SchedulerException e) {
            response.status(500);
            log.error("Error in delete job metadata!", e);
            return e.getMessage();
        }
    }

    /**
     * Method for deleting user selected anomaly jobs into database.
     *
     * @param request  Request for an anomaly report
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteSelectedJobs(Request request, Response response) {
        Set<String> jobIds = Arrays.stream(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER)).collect(Collectors.toSet());
        log.info("Getting job list from database to delete the job [{}]", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        try {
            schedulerService.stopJob(jobIds);
            jobAccessor.deleteJobs(jobIds);
            return Constants.SUCCESS;
        } catch (IOException | SchedulerException e) {
            response.status(500);
            log.error("Error in delete job metadata!", e);
            return e.getMessage();
        }
    }

    /**
     * Method called upon request for active jobs list.
     *
     * @param request  Request for jobs list
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewJobsList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting job list from database");
            params.put("jobs", jobAccessor.getJobMetadataList());
            params.put("timelineData", new JobTimeline().getCurrentTimelineJson(Granularity.HOUR));
            params.put(Constants.TITLE, "Active Jobs");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting job lists!", e);
        }
        return new ModelAndView(params, "listJobs");
    }

    /**
     * Method called upon request for deleted jobs list.
     *
     * @param request  Request for jobs list
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewDeletedJobsList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting deleted job list from database.");
            params.put("jobs", deletedJobAccessor.getDeletedJobMetadataList());
            params.put(Constants.TITLE, "Deleted Jobs");
            params.put(Constants.DELETEDJOBSVIEW, "true");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting deleted job lists!", e);
        }
        return new ModelAndView(params, "listJobs");
    }

    /**
     * Method called upon request for a job info page.
     *
     * @param request  Request for job info page
     * @param response Response
     * @return template view route
     * @throws IOException IO exception
     */
    public static ModelAndView viewJobInfo(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting job from database.");
            JobMetadata job = jobAccessor.getJobMetadata(request.params(Constants.ID));
            params.put("job", job);
            params.put("clusterName", clusterAccessor.getDruidCluster(job.getClusterId()).getClusterName());
            params.put(Constants.TITLE, "Job Details");
            params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
            params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
            params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
            params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
            params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
            params.put(Constants.MINUTE, Constants.MAX_MINUTE);
            params.put(Constants.HOUR, Constants.MAX_HOUR);
            params.put(Constants.DAY, Constants.MAX_DAY);
            params.put(Constants.WEEK, Constants.MAX_WEEK);
            params.put(Constants.MONTH, Constants.MAX_MONTH);
            params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
            params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting job info!", e);
        }
        return new ModelAndView(params, "jobInfo");
    }

    /**
     * Method called upon request for a deleted job info page.
     *
     * @param request  Request for job info page
     * @param response Response
     * @return template view route
     * @throws IOException IO exception
     */
    public static ModelAndView viewDeletedJobInfo(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting deleted job Info from database.");
            JobMetadata job = deletedJobAccessor.getDeletedJobMetadata(request.params(Constants.ID));
            params.put("job", job);
            params.put("clusterName", clusterAccessor.getDruidCluster(job.getClusterId()).getClusterName());
            params.put(Constants.TITLE, "Deleted Job Details");
            params.put(Constants.DELETEDJOBSVIEW, "true");
        } catch (Exception e) {
            // add the error to the params
            params.put(Constants.ERROR, e.getMessage());
            log.error("Error in getting deleted job info!", e);
        }
        return new ModelAndView(params, "jobInfo");
    }

    /**
     * Helper method to validate input email-ids from user.
     * @return true if email input field is valid else false
     */
    private static boolean validEmail(String emails, EmailService emailService) {
        return emails == null || emails.isEmpty() || emailService.validateEmail(emails, emailService.getValidDomainsFromSettings());
    }

    /**
     * Method for updating anomaly job info into database.
     *
     * @param request  Request for updating a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String updateJobInfo(Request request, Response response) throws IOException {
        String jobId = request.params(Constants.ID);
        log.info("Updating job metadata [{}]", jobId);
        try {
            // Parse user request and get existing job
            UserQuery userQuery = new Gson().fromJson(request.body(), UserQuery.class);
            // Validate user email
            EmailService emailService = serviceFactory.newEmailServiceInstance();
            if (!validEmail(userQuery.getOwnerEmail(), emailService)) {
                throw new SherlockException("Invalid owner email passed");
            }
            JobMetadata currentJob = jobAccessor.getJobMetadata(jobId);
            // Validate query change if any
            Query query = null;
            String newQuery = userQuery.getQuery().replaceAll(Constants.WHITESPACE_REGEX, "");
            String oldQuery = currentJob.getUserQuery().replaceAll(Constants.WHITESPACE_REGEX, "");
            if (!oldQuery.equals(newQuery)) {
                log.info("Validating altered user query");
                DruidQueryService queryService = serviceFactory.newDruidQueryServiceInstance();
                query = queryService.build(userQuery.getQuery(), Granularity.getValue(userQuery.getGranularity()), userQuery.getGranularityRange(), null, userQuery.getTimeseriesRange());
            }
            JobMetadata updatedJob = new JobMetadata(userQuery, query);
            boolean isRerunRequired = (currentJob.userQueryChangeSchedule(userQuery) || query != null) && currentJob.isRunning();
            currentJob.update(updatedJob);
            // reschedule if needed and store in the database
            if (isRerunRequired) {
                log.info("Scheduling the job with new granularity and/or new frequency and/or new query.");
                schedulerService.stopJob(currentJob.getJobId());
                schedulerService.scheduleJob(currentJob);
            }
            jobAccessor.putJobMetadata(currentJob);
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while updating the job Info!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for launching anomaly job.
     *
     * @param request  Request for launching a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String launchJob(Request request, Response response) throws IOException {
        log.info("Launching the job requested by user.");
        JobMetadata jobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(request.params(Constants.ID));
            DruidCluster cluster = clusterAccessor.getDruidCluster(jobMetadata.getClusterId());
            jobMetadata.setHoursOfLag(cluster.getHoursOfLag());
            log.info("Scheduling job.");
            // schedule the job
            schedulerService.scheduleJob(jobMetadata);
            // change the job status as running
            jobMetadata.setJobStatus(JobStatus.RUNNING.getValue());
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            // return
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while launching the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for launching selected anomaly jobs.
     *
     * @param request  Request for launching selected jobs
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String launchSelectedJobs(Request request, Response response) throws IOException {
        Set<String> jobIds = Arrays.stream(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER)).collect(Collectors.toSet());
        log.info("Launching the jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        JobMetadata jobMetadata;
        for (String id : jobIds) {
            try {
                // get jobinfo from database
                jobMetadata = jobAccessor.getJobMetadata(id);
                if (jobMetadata.getJobStatus().equalsIgnoreCase(JobStatus.RUNNING.getValue()) ||
                    jobMetadata.getJobStatus().equalsIgnoreCase(JobStatus.NODATA.getValue())) {
                    continue;
                }
                DruidCluster cluster = clusterAccessor.getDruidCluster(jobMetadata.getClusterId());
                jobMetadata.setHoursOfLag(cluster.getHoursOfLag());
                log.info("Scheduling job.");
                // schedule the job
                schedulerService.scheduleJob(jobMetadata);
                // change the job status as running
                jobMetadata.setJobStatus(JobStatus.RUNNING.getValue());
                jobAccessor.putJobMetadata(jobMetadata);
            } catch (Exception e) {
                log.error("Exception while launching the jobs {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Method for stopping anomaly job.
     *
     * @param request  Request for stopping a job
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String stopJob(Request request, Response response) throws IOException {
        log.info("Stopping the job requested by user.");
        JobMetadata jobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(request.params(Constants.ID));
            // stop the job in the sheduler
            schedulerService.stopJob(jobMetadata.getJobId());
            // change the job status to stopped
            jobMetadata.setJobStatus(JobStatus.STOPPED.getValue());
            jobAccessor.putJobMetadata(jobMetadata);
            response.status(200);
            // return
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method for stopping selected anomaly jobs.
     *
     * @param request  Request for stopping selected jobs
     * @param response Response
     * @return Nothing on success (200 status), or error message (500 status)
     * @throws IOException IO exception
     */
    public static String stopSelectedJobs(Request request, Response response) throws IOException {
        Set<String> jobIds = Arrays.stream(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER)).collect(Collectors.toSet());
        log.info("Stopping the jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        JobMetadata jobMetadata;
        for (String id : jobIds) {
            try {
                // get jobinfo from database
                jobMetadata = jobAccessor.getJobMetadata(id);
                // stop the job in the sheduler
                schedulerService.stopJob(jobMetadata.getJobId());
                // change the job status to stopped
                jobMetadata.setJobStatus(JobStatus.STOPPED.getValue());
                jobAccessor.putJobMetadata(jobMetadata);
            } catch (Exception e) {
                log.error("Exception while stopping the job {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Method for cloning anomaly job.
     *
     * @param request  Request for cloning a job
     * @param response Response
     * @return cloned jobId
     * @throws IOException IO exception
     */
    public static String cloneJob(Request request, Response response) throws IOException {
        log.info("Cloning the job...");
        JobMetadata jobMetadata;
        JobMetadata clonedJobMetadata;
        try {
            // get jobinfo from database
            jobMetadata = jobAccessor.getJobMetadata(request.params(Constants.ID));
            // copy the job metadata
            clonedJobMetadata = JobMetadata.copyJob(jobMetadata);
            clonedJobMetadata.setJobStatus(JobStatus.CREATED.getValue());
            clonedJobMetadata.setTestName(clonedJobMetadata.getTestName() + Constants.CLONED);
            clonedJobMetadata.setJobId(null);
            String clonnedJobId = jobAccessor.putJobMetadata(clonedJobMetadata);
            response.status(200);
            return clonnedJobId;
        } catch (Exception e) {
            log.error("Exception while cloning the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Rerun the job for given timestamp.
     * Request consist of job id and timestamp
     * @param request HTTP request
     * @param response HTTP response
     * @return status of request
     */
    public static String rerunJob(Request request, Response response) {
        try {
            Map<String, String> params = new Gson().fromJson(
                request.body(),
                new TypeToken<Map<String, String>>() { }.getType()
            );
            String jobId = params.get("jobId");
            JobMetadata job = jobAccessor.getJobMetadata(jobId);
            long start = Long.valueOf(params.get("timestamp"));
            long end = start + Granularity.getValue(job.getGranularity()).getMinutes();
            serviceFactory.newJobExecutionService().performBackfillJob(job, TimeUtils.zonedDateTimeFromMinutes(start), TimeUtils.zonedDateTimeFromMinutes(end));
            response.status(200);
            return Constants.SUCCESS;
        } catch (SherlockException | IOException | JobNotFoundException e) {
            log.error("Error occurred during job backfill!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method to view cron job reports.
     *
     * @param request  User request to view report
     * @param response Response
     * @return template view route
     */
    public static ModelAndView viewJobReport(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // Get the job id
        String jobId = request.params(Constants.ID);
        String frequency = request.params(Constants.FREQUENCY_PARAM);
        // Get the json timeline data from database
        try {
            List<AnomalyReport> report = reportAccessor.getAnomalyReportsForJob(jobId, frequency);
            JsonTimeline jsonTimeline = Utils.getAnomalyReportsAsTimeline(report);
            String jsonTimelinePoints = new Gson().toJson(jsonTimeline.getTimelinePoints());
            JobMetadata jobMetadata = jobAccessor.getJobMetadata(jobId);
            // populate params for visualization
            params.put(Constants.JOB_ID, jobId);
            params.put(Constants.FREQUENCY, frequency);
            params.put(Constants.HOURS_OF_LAG, jobAccessor.getJobMetadata(jobId).getHoursOfLag());
            params.put(Constants.TIMELINE_POINTS, jsonTimelinePoints);
            params.put(Constants.TITLE, jobMetadata.getTestName());
        } catch (Exception e) {
            log.error("Error while viewing job report!", e);
            params.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(params, "report");
    }

    /**
     * Method to send job report as requested by users based on datetime.
     *
     * @param request  Job report request
     * @param response Response
     * @return report HTML as a string to render on UI
     */
    @SuppressWarnings("unchecked")
    public static String sendJobReport(Request request, Response response) {
        log.info("Processing user request for reports.");
        Map<String, Object> params = new HashMap<>();
        try {
            // parse the json response to get requested datetime of the report
            Map<String, String> requestParamsMap = new Gson().fromJson(request.body(), Map.class);
            String selectedDate = requestParamsMap.get(Constants.SELECTED_DATE);
            String frequency = requestParamsMap.get(Constants.FREQUENCY);
            // get the report from the database
            List<AnomalyReport> reports = reportAccessor.getAnomalyReportsForJobAtTime(
                    request.params(Constants.ID),
                    selectedDate,
                    frequency
            );
            List<AnomalyReport> anomalousReports = new ArrayList<>(reports.size());
            for (AnomalyReport report : reports) {
                if (report.getAnomalyTimestamps() != null && !report.getAnomalyTimestamps().isEmpty()) {
                    anomalousReports.add(report);
                }
            }
            params.put(DatabaseConstants.ANOMALIES, anomalousReports);
            // render the table HTML of the report
            String tableHtml = thymeleaf.render(new ModelAndView(params, "table"));
            response.status(200);
            // return table HTML as a string
            return tableHtml;
        } catch (Exception e) {
            log.error("Exception while preparing reports to send!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * This route returns the add new Druid cluster form.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the Druid cluster form
     */
    public static ModelAndView viewNewDruidClusterForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "druidForm");
    }

    /**
     * This route adds a new Druid cluster to the database.
     *
     * @param request  the HTTP request containing the cluster parameters
     * @param response HTTP response
     * @return the new cluster ID
     */
    public static String addNewDruidCluster(Request request, Response response) {
        log.info("Adding a new Druid cluster");
        DruidCluster cluster;
        try {
            // Parse druid cluster params
            String requestJson = request.body();
            log.debug("Received Druid cluster params: {}", requestJson);
            cluster = new Gson().fromJson(requestJson, DruidCluster.class);
            cluster.validate(); // validate parameters
            cluster.setClusterId(null);
            // Store in database
            clusterAccessor.putDruidCluster(cluster);
            response.status(200);
            return cluster.getClusterId().toString();
        } catch (SherlockException | IOException e) {
            log.error("Error occured while adding Druid Cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method returns a populated table of the current Druid clusters.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return list view of Druid clusters
     */
    public static ModelAndView viewDruidClusterList(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        try {
            log.info("Getting list of Druid clusters from database");
            params.put("clusters", clusterAccessor.getDruidClusterList());
            params.put(Constants.TITLE, "Druid Clusters");
        } catch (Exception e) {
            log.error("Fatal error occured while retrieving Druid clusters!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "druidList");
    }

    /**
     * Method returns a view of a specified Druid cluster's info.
     *
     * @param request  HTTP request containing the cluster ID
     * @param response HTTP response
     * @return view of the Druid cluster's info
     */
    public static ModelAndView viewDruidCluster(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Druid Cluster Details");
        try {
            params.put("cluster", clusterAccessor.getDruidCluster(request.params(Constants.ID)));
            log.info("Cluster retrieved successfully");
        } catch (Exception e) {
            log.error("Fatal error while retrieving cluster!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "druidInfo");
    }

    /**
     * Exposed method to delete a Druid cluster with a specified ID.
     *
     * @param request  HTTP request containing the cluster ID param
     * @param response HTTP response
     * @return success if the cluster was deleted or else an error message
     */
    public static String deleteDruidCluster(Request request, Response response) {
        String clusterId = request.params(Constants.ID);
        log.info("Deleting cluster with ID {}", clusterId);
        try {
            List<JobMetadata> associatedJobs = jobAccessor.getJobsAssociatedWithCluster(clusterId);
            if (associatedJobs.size() > 0) {
                log.info("Attempting to delete a cluster that has {} associated jobs", associatedJobs.size());
                response.status(400);
                return String.format("Cannot delete cluster with %d associated jobs", associatedJobs.size());
            }
            clusterAccessor.deleteDruidCluster(clusterId);
            response.status(200);
            return Constants.SUCCESS;
        } catch (IOException | ClusterNotFoundException e) {
            log.error("Error while deleting cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Update a Druid cluster with a specified ID and new parameters.
     *
     * @param request  HTTP request containing the cluster ID and new cluster parameters
     * @param response HTTP response
     * @return 'success' or an error message
     */
    public static String updateDruidCluster(Request request, Response response) {
        String clusterId = request.params(Constants.ID);
        log.info("Updating cluster with ID {}", clusterId);
        DruidCluster existingCluster;
        DruidCluster updatedCluster;
        try {
            existingCluster = clusterAccessor.getDruidCluster(clusterId);
            updatedCluster = new Gson().fromJson(request.body(), DruidCluster.class);
            updatedCluster.validate();
            boolean requireReschedule = !existingCluster.getHoursOfLag().equals(updatedCluster.getHoursOfLag());
            existingCluster.update(updatedCluster);
            // Put updated cluster in DB
            clusterAccessor.putDruidCluster(existingCluster);
            if (requireReschedule) {
                log.info("Hours of lag has changed, rescheduling jobs for cluster");
                List<JobMetadata> rescheduleJobs = jobAccessor
                        .getRunningJobsAssociatedWithCluster(existingCluster.getClusterId());
                for (JobMetadata job : rescheduleJobs) {
                    job.setHoursOfLag(existingCluster.getHoursOfLag());
                }
                schedulerService.stopAndReschedule(rescheduleJobs);
                jobAccessor.putJobMetadata(rescheduleJobs);
            }
            log.info("Druid cluster updated successfully");
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Fatal error while updating Druid cluster!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * This method will get the entire backend database as a JSON
     * string and return it to the caller.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the backend database as a JSON string dump
     */
    public static String getDatabaseJsonDump(Request request, Response response) {
        try {
            JsonObject obj = jsonDumper.getRawData();
            return new Gson().toJson(obj);
        } catch (Exception e) {
            log.error("Error while getting backend json dump!", e);
            return e.getMessage();
        }
    }

    /**
     * A post request to this method where the request body
     * is the JSON string that specifies what to add
     * to the backend database using the JSON dumper.
     *
     * @param request  HTTP request whose body is the JSON
     * @param response HTTP response
     * @return 'OK' if the write is successful, else an error message
     */
    public static String writeDatabaseJsonDump(Request request, Response response) {
        String json = request.body();
        try {
            JsonParser parser = new JsonParser();
            JsonObject obj = parser.parse(json).getAsJsonObject();
            jsonDumper.writeRawData(obj);
            response.status(200);
            return "OK";
        } catch (Exception e) {
            response.status(500);
            log.error("Error while writing JSON to backend!", e);
            return e.getMessage();
        }
    }

    /**
     * Method to view meta settings page.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return object of ModelAndView
     */
    public static ModelAndView viewSettings(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Meta Manager");
        try {
            params.put("jobs", jobAccessor.getJobMetadataList());
            params.put("queuedJobs", jsonDumper.getQueuedJobs());
            params.put("emails", emailMetadataAccessor.getAllEmailMetadata());
        } catch (Exception e) {
            log.error("Fatal error while retrieving settings!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "settings");
    }

    /**
     * Method to view metadata about selected email.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return object of ModelAndView
     */
    public static ModelAndView viewEmails(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Email Settings");
        try {
            List<String> triggers = Triggers.getAllValues();
            triggers.remove(Triggers.MINUTE.toString());
            params.put("emailTriggers", triggers);
            params.put("email", emailMetadataAccessor.getEmailMetadata(request.params(Constants.ID)));
        } catch (Exception e) {
            log.error("Fatal error while retrieving email settings!", e);
            params.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(params, "emailInfo");
    }

    /**
     * Method to update email metadata as requested.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return emailid as string
     */
    public static String updateEmails(Request request, Response response) {
        String emailId = request.params(Constants.ID);
        log.info("Updating email metadata for [{}]", emailId);
        try {
            EmailMetaData newEmailMetaData = new Gson().fromJson(request.body(), EmailMetaData.class);
            EmailMetaData oldEmailMetadata = emailMetadataAccessor.getEmailMetadata(newEmailMetaData.getEmailId());
            if (!newEmailMetaData.getRepeatInterval().equalsIgnoreCase(oldEmailMetadata.getRepeatInterval())) {
                emailMetadataAccessor.removeFromTriggerIndex(newEmailMetaData.getEmailId(), oldEmailMetadata.getRepeatInterval());
            }
            emailMetadataAccessor.putEmailMetadata(newEmailMetaData);
            response.status(200);
            return emailId;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Method to delete email metadata as requested.
     * @param request  HTTP request
     * @param response HTTP response
     * @return status string
     */
    public static String deleteEmail(Request request, Response response) {
        String emailId = request.params(Constants.ID);
        log.info("Deleting email metadata for [{}]", emailId);
        try {
            EmailMetaData emailMetadata = emailMetadataAccessor.getEmailMetadata(emailId);
            jobAccessor.deleteEmailFromJobs(emailMetadata);
            response.status(200);
            return Constants.SUCCESS;
        } catch (Exception e) {
            log.error("Exception while stopping the job!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Shows an advanced instant query view.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return query form
     * @throws IOException if an error occurs while getting the cluster list
     */
    public static ModelAndView debugInstantReport(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        return new ModelAndView(params, "debugForm");
    }

    /**
     * Processe a power query and store the results.
     * Send the job and query ID back to the UI.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return `[queryId]-[jobId]`
     */
    public static ModelAndView debugPowerQuery(Request request, Response response) {
        Map<String, Object> modelParams = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        modelParams.put(Constants.TITLE, "Power Query");
        try {
            Map<String, String> params = Utils.queryParamsToStringMap(request.queryMap());
            Query query = QueryBuilder.start()
                    .endAt(params.get("endTime"))
                    .startAt(params.get("startTime"))
                    .queryString(params.get("query"))
                    .granularity(params.get("granularity"))
                    .intervals(params.get("intervals"))
                    .build();
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            JobMetadata job = new JobMetadata(userQuery, query);
            JobExecutionService executionService = serviceFactory.newJobExecutionService();
            DruidCluster cluster = clusterAccessor.getDruidCluster(job.getClusterId());
            List<Anomaly> anomalies = executionService.executeJob(job, cluster, query);
            List<AnomalyReport> reports = executionService.getReports(anomalies, job);
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            String tableHtml = thymeleaf.render(new ModelAndView(tableParams, "table"));
            modelParams.put("tableHtml", tableHtml);
        } catch (Exception e) {
            log.error("Exception", e);
            modelParams.put(Constants.ERROR, e.getMessage());
        }
        return new ModelAndView(modelParams, "reportInstant");
    }

    /**
     * Send the form used to perform backfill jobs.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return backfill form view
     * @throws IOException if there was an error getting the job list
     */
    public static ModelAndView debugBackfillForm(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put("jobs", jobAccessor.getJobMetadataList());
        return new ModelAndView(params, "debugBackfill");
    }

    /**
     * Process and run a backfill job.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return an error message or 'success'
     */
    public static String debugRunBackfillJob(Request request, Response response) {
        try {
            Map<String, String> params = new Gson().fromJson(
                    request.body(),
                    new TypeToken<Map<String, String>>() { }.getType()
            );
            String[] jobIds = params.get("jobId").split(Constants.COMMA_DELIMITER);
            ZonedDateTime startTime = TimeUtils.parseDateTime(params.get("fillStartTime"));
            ZonedDateTime endTime = ("".equals(params.get("fillEndTime")) || params.get("fillEndTime") == null) ? null : TimeUtils.parseDateTime(params.get("fillEndTime"));
            for (String jobId : jobIds) {
                JobMetadata job = jobAccessor.getJobMetadata(jobId);
                serviceFactory.newJobExecutionService().performBackfillJob(job, startTime, endTime);
            }
            response.status(200);
            return "Success";
        } catch (SherlockException | IOException | JobNotFoundException e) {
            log.error("Error occurred during job backfill!", e);
            response.status(500);
            return e.getMessage();
        }
    }

    /**
     * Endpoint used to clear all reports associated with a job.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message is something goes wrong
     */
    public static String debugClearJobReports(Request request, Response response) {
        try {
            reportAccessor.deleteAnomalyReportsForJob(request.params(Constants.ID));
            return "Success";
        } catch (Exception e) {
            log.error("Error clearing job reports!", e);
            return e.getMessage();
        }
    }

    /**
     * Endpoint used to clear all reports associated with given jobs.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message is something goes wrong
     */
    public static String clearReportsOfSelectedJobs(Request request, Response response) {
        Set<String> jobIds = Arrays.stream(request.params(Constants.IDS).split(Constants.COMMA_DELIMITER)).collect(Collectors.toSet());
        log.info("Clearing reports of jobs id:[{}] requested by user", jobIds.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
        for (String id : jobIds) {
            try {
                reportAccessor.deleteAnomalyReportsForJob(id);
            } catch (Exception e) {
                log.error("Error clearing reports of the jobs {}!", id, e);
                response.status(500);
                return e.getMessage();
            }
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    /**
     * Endpoint used to clear jobs created from the debug interface.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return 'success' or an error message
     */
    public static String debugClearDebugJobs(Request request, Response response) {
        try {
            jobAccessor.deleteDebugJobs();
            return "Success";
        } catch (IOException e) {
            return e.getMessage();
        }
    }

    /**
     * Display a query page with EGADS configurable params.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return query page
     * @throws IOException if an array occurs while getting the clusters
     */
    public static ModelAndView debugShowEgadsConfigurableQuery(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        params.put("filteringMethods", EgadsConfig.FilteringMethod.values());
        return new ModelAndView(params, "debugMegaQuery");
    }

    /**
     * Perform an EGADS-configured query. This method will get the
     * EGADS configuration parameters from the request body
     * and build an {@code EgadsConfig} object.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return the query ID and job ID
     */
    public static ModelAndView debugPerformEgadsQuery(Request request, Response response) {
        Map<String, Object> modelParams = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        modelParams.put(Constants.TITLE, "Egads Query");
        try {
            Map<String, String> params = Utils.queryParamsToStringMap(request.queryMap());
            Query query = QueryBuilder.start()
                    .endAt(params.get("endTime"))
                    .startAt(params.get("startTime"))
                    .queryString(params.get("query"))
                    .granularity(params.get("granularity"))
                    .intervals(params.get("intervals"))
                    .build();
            EgadsConfig egadsConfig = EgadsConfig.create()
                    .maxAnomalyTimeAgo(params.get("maxAnomalyTimeAgo"))
                    .aggregation(params.get("aggregation"))
                    .timeShifts(params.get("timeShifts"))
                    .baseWindows(params.get("baseWindows"))
                    .period(params.get("period"))
                    .fillMissing(params.get("fillMissing"))
                    .numWeeks(params.get("numWeeks"))
                    .numToDrop(params.get("numToDrop"))
                    .dynamicParameters(params.get("dynamicParameters"))
                    .autoAnomalyPercent(params.get("autoSensitivityAnomalyPercent"))
                    .autoStandardDeviation(params.get("autoSensitivityStandardDeviation"))
                    .preWindowSize(params.get("preWindowSize"))
                    .postWindowSize(params.get("postWindowSize"))
                    .confidence(params.get("confidence"))
                    .windowSize(params.get("windowSize"))
                    .filteringMethod(params.get("filteringMethod"))
                    .filteringParam(params.get("filteringParam"))
                    .build();
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            JobMetadata job = new JobMetadata(userQuery, query);
            JobExecutionService executionService = serviceFactory.newJobExecutionService();
            DetectorService detectorService = serviceFactory.newDetectorServiceInstance();
            List<EgadsResult> egadsResult = detectorService.detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    null,
                    egadsConfig
            );
            List<Anomaly> anomalies = new ArrayList<>();
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
            }
            List<AnomalyReport> reports = executionService.getReports(anomalies, job);
            response.status(200);
            Gson gson = new Gson();
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            String data = gson.toJson(EgadsResult.fuseResults(egadsResult), jsonType);
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            String tableHtml = thymeleaf.render(new ModelAndView(tableParams, "table"));
            modelParams.put("tableHtml", tableHtml);
            modelParams.put("data", data);
        } catch (Exception e) {
            log.error("Exception", e);
            modelParams.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(modelParams, "reportInstant");
    }

    /**
     * Method to view redis restore form.
     * @param request HTTP request
     * @param response HTTP response
     * @return redisRestoreForm html
     * @throws IOException exception
     */
    public static ModelAndView restoreRedisDBForm(Request request, Response response) throws IOException {
        Map<String, Object> params = new HashMap<>(defaultParams);
        return new ModelAndView(params, "redisRestoreForm");
    }

    /**
     * Method to process the restore of redis DB from given json file.
     * @param request HTTP request
     * @param response HTTP response
     * @return request status 'success' or error
     */
    public static String restoreRedisDB(Request request, Response response) {
        Map<String, String> params = new Gson().fromJson(request.body(), new TypeToken<Map<String, String>>() { }.getType());
        String filePath = params.get(Constants.PATH);
        try {
            schedulerService.removeAllJobsFromQueue();
        } catch (SchedulerException e) {
            log.error("Error while unscheduling current jobs!", e);
            response.status(500);
            return e.getMessage();
        }
        try {
            jsonDumper.writeRawData(BackupUtils.getDataFromJsonFile(filePath));
        } catch (IOException e) {
            log.error("Unable to load data from the file at {} ", filePath, e);
            response.status(500);
            return e.getMessage();
        }
        response.status(200);
        return Constants.SUCCESS;
    }

    public static ModelAndView viewAccountInstantAnomalyJobForm(Request request, Response response) {
        Map<String, Object> params = new HashMap<>(defaultParams);
        // set instant form view
        params.put(Constants.INSTANTVIEW, "true");
        params.put(Triggers.MINUTE.toString(), CLISettings.INTERVAL_MINUTES);
        params.put(Triggers.HOUR.toString(), CLISettings.INTERVAL_HOURS);
        params.put(Triggers.DAY.toString(), CLISettings.INTERVAL_DAYS);
        params.put(Triggers.WEEK.toString(), CLISettings.INTERVAL_WEEKS);
        params.put(Triggers.MONTH.toString(), CLISettings.INTERVAL_MONTHS);
        params.put(Constants.MINUTE, Constants.MAX_MINUTE);
        params.put(Constants.HOUR, Constants.MAX_HOUR);
        params.put(Constants.DAY, Constants.MAX_DAY);
        params.put(Constants.WEEK, Constants.MAX_WEEK);
        params.put(Constants.MONTH, Constants.MAX_MONTH);
        params.put(Constants.TIMESERIES_MODELS, EgadsConfig.TimeSeriesModel.getAllValues());
        params.put(Constants.ANOMALY_DETECTION_MODELS, EgadsConfig.AnomalyDetectionModel.getAllValues());
//        params.put("accounts",Arrays.asList("Top10Ratings.com (Newsletter)","InvestingBytes.com Webpush Monetization","AutoArena.com Webpush Monetization","Thunderdeals.net Webpush Monetization","Healthaccess.com Webpush Monetization","MobilesArena.com Webpush Monetization","BlackFridayMart.com Webpush Monetization","Shop.Local.com Webpush Monetization","Top10Ratings.com WebPush","Find.Local.com WebPush Monetization","Local.com Webpush Monetization","local.com (site)","BlackFridayClub.net (Site n Blog)","BlackFridayStar.net (Site n Blog)","CyberMondayNow.com (Site n Blog)","CyberMondayDaily.com (Site n Blog)","CyberMondayClub.net (Site n Blog)","BlackFridaySteals.net (Site n Blog)","CyberMonday365.com (Site n Blog)","ElectronicsInsider.net (Site n Blog)","Swiftfeed.Net (Site n Blog)","Localwizard.net (Site n Blog)","Swipewiki.com (Site n Blog)","TheDiscountMill.com (Site n Blog)","Advicepool.net (Site n Blog)","Resultsfeed.net (Site n Blog)","BlackFridayFest.net (Site n Blog)","Cybermondaycart.com (Site n Blog)","Cybermondaypicks.com (Site n Blog)","Blackfridaypicks.net (Site n Blog)","Resultsdigest.com (Site n Blog)","BlackFridayDepot.com (Site n Blog)","Resultspanda.com (Site n Blog)","Localinsights.net (Site n Blog)","Resultsinsights.com(Site n Blog)","Healthylivingly.com - Site","Naturalremedees - Site","FeelingWell.online - Site","Resultsbee.com (Site n Blog)","Top10Ratings.com (Site and Blog)","Life-Bliss.com - Site","Shoppingtherapy.online - Site","OfertasCybermondayUS.com (Site n Blog)","UKBlackFridayDeals.com (Site n Blog)","CyberMondayStores.net (Site n Blog)","247BlackFridayDeals.com (Site n Blog)","BlackFridayDeal4U.com (Site n Blog)","ESBlackFriday.com (Site n Blog) ","UKCyberMonday.com (Site n Blog)","CyberMondayPrices.com (Site n Blog)","CyberMondayMania.net (Site n Blog)","BlackFridayPrices.net (Site n Blog)","Mindbodygreen.com - Site","Dailybundlehub.com - Site","Zoomcorner.com - Site","WebTrends.co","Nurishingly.com - Site","Glowwise.com - Site","Quelly.net (Site n Blog)","BlackFridayPrecios.com (Site n Blog)","ESCybermonday.com (Site n Blog)","BlackFridayUKSale.com (Site n Blog)","Bruckie.com (Site n Blog)","Aerlie.net (Site n Blog)","Kipen.net (Site n Blog)","AgileFeed.net (Site n Blog)","Averles.com (Site n Blog)","FRBlackFriday.com (Site n Blog)","Pazton.com (Site n Blog)","Qualdo.net (Site n Blog)","Patteo.net (Site n Blog)","Nobbyn.com (Site  n Blog)","Mortow.com (Site n Blog)","Empirewell.com - Site","Updateworld.net - Site","Namestae.com - Site","Wellagic.com - Site","Uphealthnewz.com - Site","Bestofwell.com - Site","SearchInstantly.net (Site n Blog)","Hertempo.com - Site","Naturefrsh.com - Site","Trenderset - Site","ShoppersPoint.net (Site n Blog)","InsightsPanda.com (Site n Blog)","GenieAdvice.com (Site n Blog)","KnowNemo.com (Site n Blog)","GenieSage.com (Site n Blog)","Hubwish - Site","Uberscene - Site","24/7Tempo - Site","ShoppersFest.com (Site n Blog)","OffersPod.com (Site n Blog)","OffersGarden.com (Site n Blog)","DealsDivine.com (Site n Blog)","Deals2019.com (Site n Blog)","2019Discounts.com (Site n Blog)","BlackFridayDealsUK.net (Site n Blog)","SpeakingHealth - Taboola","CyberMondayOferta.com (Site n Blog)","Answersdepot.net (Site n Blog)","HealthCareToday.net (Site n Blog)","2019Saver.net (Site n Blog)","OffersLounge.com (Site n Blog)","DealsFarms.com (Site n Blog)","2019Prices.com (Site n Blog)","Savings2019.com (Site n Blog)","BlackFridayOferta.com (Site n Blog)","CyberMondayUK.net (Site n Blog)","Healthmaester.com (Site n Blog)","InfoSavant.net (Site n Blog)","Descubrehoy.net (Site n Blog)","FinderBee.net (Site n Blog)","Muchascosas.net (Site n Blog)","Elbuscador.net (Site n Blog)","Choixatoutva.com (Site n Blog)","Superrecherche.com (Site n Blog)","Alleserdenkliche.com (Site n Blog)","Suchdichgluecklich.com (Site n Blog)","Getmoredaily.com - Site","Zoobyte.com - Site","Bespedia.com - Site","Healthtonic.net - Site","FAQs.com (Site n Blog)","Healthnacity.com - Site","Mundo.com - Site","Wellseries.com - Site","Nurishedhealth.com - Site","Livebtter.com - Site","Welldigest.com - Site","Academida.com - Site","WellCanvas - Site - results","Healthhandbook Site","Healthoholic.online - Site","Wellcanvas.com - Site","Wethinkhealth.online - Site","Wethinkhealth.online  (results) - Site","Healthoholic.online - Site","Healthhandbook - Site","Pearlywhytes.com - Site","Xmas.discount(Site n Blog)","Blackfridaydealsforyou - Site","Dealrings - Site","CyberMondayStreet.com(Site n Blog)","BlackFridayFair.com(Site n Blog)","CyberMondayUsa.com(Site n Blog)","Thinkoffers.net(Site n Blog)","offersportal.net(Site n Blog)","Bargains.blackfriday(Site n Blog)","Blackfridaysavings.com(Site n Blog)","blackfridaybargains.com(Site n Blog)","CyberMondaySales.net(Site n Blog)","SmartDiscounts.net(Site n Blog)","Cybermondayoffers.com(Site n Blog)","Gadgetoffers.com(Site n Blog)","BlackfridayStreet.com (Site n Blog)","CybermondayFest.com (Site n Blog)","divatrends.net(SITE n BLOG)","Immediatediscounts.com (Site n Blog)","Dealsmate - Site","Raiseyourstakes.co.uk-Site","Inquirily.com - Site","Autobytel - Site","Classificados - Site","Crawlerwatch - Site","Zillopedia - Site","Thebizcluster - Site","BestHealthUpdates - Site","HealthCarePanda - Site","Healthcare.com - Site","Wellnessdaily.net - Site","Livedailyhealth - Site","Instahealthdaily - Site","TVsInsider.com (Site n Blog)","Laptopsinsider.com (Site n Blog)","AllYear.deals (Site n Blog)","Savings2018.com (Site n Blog)","Thelushhouse - Site","Tipsforhealth.com (Site n Blog)","Autoswire.com (Site n Blog)","DegreesCompared.com (Site n Blog)","BuyStores.net (Site n Blog)","Sales365.net (Site n Blog)","FinderMonk.com (Site n Blog)","Thehealthcluster - Site","Cartrends.online Site","Investopoly - Site","npress.local.com","Cardswallet.com (Site n Blog)","ZipHip.com Site","Directory.site Site","ExploreLocal.net Site","MrLocal.com Site","Local.com Site DM","SeasonDeals.store(Site n Blog)","Wantbargain.com(Site n Blog)","ShopperSphere.com (Site n Blog)","Targeteddeals.com (Site n Blog)","Dealsandsteals.com(SITE n BLOG)","SelectDeals.net (Site n Blog)","Topvarsity.com (Site and Blog)","Theprettylife.online - Site","HealthAccess.com Site","Dailybuffer.online - Site","DriverCulture.online (Site)","AutoCulture.online (Site)","VitalHealth.online (Site)","PrimalHealth.online (Site)","HealthFocus.online (Site)","RealHealth.online (Site)","247WallSt.com (Site)","Empowher.com (Site)","Innerspark.online (Site)","DailyCluster.com (Site)","Herstyle.online - Site","Businesslink.online - Site","Mensfocus.online - Site","Electronicsoutlet.online - Site","Thriftydeals.com (Site)","HappyPaws.online - Site","Businessfocus.online - Site","WebMD.com (TEST)","Beyondthewire.online - Site","Jennystores.com (Site and Blog)","Findanswers.online (Site and Blog)","Knowhubb.com (Site n Blog)","InfoShoppe.net (Site n Blog)","InfoInsider.net (Site n Blog)","Deals2018.com (Site n Blog)","2018Prices.com (Site n Blog)","2018Discounts.com (Site n Blog)","Christmas.net (Site n Blog)","KitchensDeals.com (Site n Blog)","GameConsole.Deals (Site n Blog)","BlackFridayShoppe.net (Site n Blog)","USABlackFriday.com (Site n Blog)","Blackfridayelectronics.net (Site n Blog)","BlackFridayUS.net (Site n Blog)","CyberMondayay.com (Site n Blog)","Laptop.blackfriday (Site n Blog)","Mobiles.blackfriday (Site n Blog)","TvSales.BlackFriday (Site n Blog)","Allanswers.com (Site & Blog)","Gotquestion.com (Site & Blog)","Elderlytimes.com (Site & Blog)","YearEndSale.co.uk (Site And Blog)","YearEnd.Discount (Site n Blog)","XmasDiscounts.net (Blog And Site)","USPriceDrops.com (Site n Blog)","TravellersGuide.net (Site n Blog)","TheInfoDigest.com (Site n Blog)","SpeakingHealth.com (Site)","ShopDealio.co.uk (Site n Blog)","SaverPrices.net (Site n Blog)","Focusedbiz.com (Site)","Deals2017.net (Site n Blog)","CheapestDeals.net (Site n Blog)","BlackFridayay.com (Site n Blog)","2017prices.com (Site n Blog)","2017discounts.com (Site n Blog)","USDealSteals.com (Site n Blog)","ShopDealio.net (Site n Blog)","SavingPanda.com (Site n Blog)","SaverDeals.net (Site n Blog)","Listscoop.com (Site n Blog)","HomeGardenHub.net (Site n Blog)","CompareMarts.com (Site n Blog)","CheapMunks.com (Site n Blog)","CheapBargain.net (Site n Blog)","BuyNation.net (Blog n Site)","2016Discounts.com (Site n Blog)","Topichomes.com(Site n Blog)","Connexity","TravellersGuide.net Adx","Local.com MediaMath","Unassigned","Dev Testv","MSN Test Customer 1","MSN Test Customer 2","MSN Test Customer 3","MSN Test Customer 4","MSN Test Customer 5","vDev Test","GotQuestion.com(DM)","Elitesavings.net (MAX)","Local.com Skenzo 0 Click","MobilesWizard.com (MAX)","Elderlytimes.com (MAX)","Allanswers.com (MAX)","MSN Auto (MAX)","MSN Health (MAX)","Top10ratings.com B53","Top10Ratings.com Max","Local.com Skenzo 1 Click","Articles.Local.com Max Web Push","Comparecards.online (MAX)","Gotquestion.com (MAX)","Local.com SEM 1 Click","Searchanswers.net (Max - Bodis)","Searchanswers.net (Max - Sedo)","MobilesInsider.com (Propel Media)","DegreesCompared.com (Propel Media)","Knowhubb.com (Propel Media - MAX)","AllYearOffers.com Site Propel","AllYearOffers.com (AMZ)","HealthInsider.net (Propel Media)","Healthpixie.net (Propel Media)","Top10ratings.com (Propel Media)","TheHealthyWorld.net (Propel Media)","AllYearOffers.com (Advertise)","health.online - RevContent","InnerSpark - Revcontent","Zillopedia.com - Revcontent","Dailybuffer.com - Revcontent","Realhealth.online - Revcontent","Crawlerwatch.com - Revcontent","Herstyle.online - Revcontent","Mensfocus.online - Revcontent","Vitalhealth.online - Revcontent","Speakinghealth.com - Revcontent","247WallSt.com - RevContent","Autobytel - Revcontent","HappyPaws.online - RevContent","HealthCare.com 1 - RevContent","HealthCare.com 2 - RevContent","WellnessDaily.net - RevContent","InstaHealthDaily.com - RevContent","LiveDailyHealth.com - RevContent","HerStyle.online - YieldMo","DriverCulture.online - YieldMo","Crawlerwatch - Snapchat","Herstyle - Pinterest","Empowher - Pinterest","HealthyFi.net (Twitter)","FAQs.com (Twitter)","Beyondthewire.online - Taboola","Cartrends.online - Taboola (Not in use)","Businessfocus.online - Taboola","HappyPaws.online - Taboola","Electronicsoutlet.online - Taboola","Businesslink.online - Taboola","Herstyle.online - Taboola","Mensfocus.online - Taboola","Local.com Taboola","iCanAnswerThat.com Taboola","Allanswers.com(Taboola)","TheHealthDiary.com (Taboola)","InnerSpark.online - Taboola","HealthAccess.com Taboola","MobilesInsider.com (Taboola)","QualityHealth.com (Taboola)","EliteSavings.net (Taboola)","Healthpixie.net (Taboola)","Cardswallet.com(Taboola)","Autos.MrLocal.com Taboola","Dailycluster.com - Taboola","PrimalHealth.online- Taboola","VitalHealth.online- Taboola","HealthFocus.online- Taboola","RealHealth.online- Taboola","AutoCulture.online - Taboola","DriverCulture.online - Taboola","247WallSt.com - Taboola","Theprettylife.online - Taboola","Empowher.com - Taboola","KnowHubb.com (Taboola)","GenieSearch.net (Taboola)","SearchInsider.net (Taboola)","JeevesKnows.com (Taboola)","TheLifeDigest.com (Taboola)","Investopoly - Taboola","Thelushhouse - Taboola","InfoShoppe.net (Taboola)","Thehealthcluster - Taboola","Cartrends.online - Taboola","Tipsforhealth.com(Taboola)","DegreesCompared.com (Taboola)","MrLocal.com Taboola","ExploreLocal.net Taboola","InfoInsider.net (Taboola)","FinderMonk.com (Taboola)","Answerly.net Taboola","InsightsAuto.com (Taboola)","Healthinsider.net Taboola","HealthAtoZ.net Taboola","Instahealthdaily - Taboola","Livedailyhealth - Taboola","Wellnessdaily.net - Taboola","Dm Taboola - Legal - PP","Dm Taboola - Pets - PP","Dm Taboola - Autos - PP","Dm Taboola - Seniors - PP","Dm Taboola - Finance - PP","Dm Taboola - Health - PP","Dm Taboola - Internet & Telecom - PP","Dm Taboola - Consumer Electronics - PP","Dm Taboola - Jobs - PP","Dm Taboola - Shopping - PP","Dm Taboola - Business - PP","Dm Taboola - Travel - PP","Dm Taboola - Real Estate - PP","Dm Taboola - Home & Garden - PP","Dm Taboola - Sports & Fitness - PP","Dm Taboola - Beauty & Personal Care - PP","Thebizcluster - Taboola","Mobiles247.net (Taboola)","Theprettylife.online 2 - Taboola","Dailybuffer.online - Taboola","Topvarsity.com(Taboola - Perform)","Crawlerwatch - Taboola","Zillopedia - Taboola","CarAndDriver.com (Taboola)","Classificados - Taboola","Healthcare.com - Taboola","BlackFridayHub.net (Taboola)","DealsBlackFriday.net (Taboola)","CyberMondayStore.net (Taboola)","CyberMondayUS.com (Taboola)","DealsPanda.net (Taboola)","AllYear.Deals (Taboola)","Raiseyourstakes.co.uk-Taboola","Autobytel - Taboola","TravellersGuide.net (Taboola)","Divatrends.net (Taboola)","ZipHip.com (Taboola)","BusinessBytes.net (Taboola)","Investingbytes.com (Taboola)","Gotquestion.com (Taboola)","FAQsLibrary.com (Taboola)","Articles.Local.com Taboola","Searchanswers.net (Taboola - Sedo)","Local.com 3 Taboola","Top10Ratings.com Taboola","BargainArena.net (Taboola)","CheapMunks.com (Taboola)","HealthyFi.net (Taboola)","Inquirily.com - RevContent","Raiseyourstakes.com - RevContent","Parkingcrew - Revcontent","HealthInsider.net Revcontent","HealthHandbook.online - RevContent","Classificados.com - RevContent","HealthoHolic.online - RevContent","WellCanvas.com - RevContent","DealyScoop.com - RevContent","RetailHolics.com - RevContent","FinderMonk.com (RevContent)","Livebtter.com - Revcontent","DailyBuffer.online 2 - RevContent","Wellseries - Revcontent","Nurishedhealth - Revcontent","Healthtonic.net - Revcontent","Welldigest.com - Revcontent","Mundo.com - Revcontent","WellCanvas.com 2 - RevContent","Healthnacity.com - RevContent","Zoobyte.com - Revcontent - MT","Getmoredaily.com - Revcontent","ZoomCorner - RevContent - PS","EmpireWell - RevContent - PB","FAQs.com (Taboola)","2019Discounts.com (Taboola)","2019Prices.com (Taboola)","Deals2019.com (Taboola)","DealsFarms.com (Taboola)","DealsDivine.com (Taboola)","OffersPod.com (Taboola)","ShoppersFest.com (Taboola)","ShoppersPoint.net (Taboola)","OffersLounge.com (Taboola)","OffersGarden.com (Taboola)","GenieSage.com (Taboola)","KnowNemo.com (Taboola)","SearchInstantly.net (Taboola)","GenieAdvice.com (Taboola)","InsightsPanda.com (Taboola)","Getmoredaily.com - Taboola","MSN Websearch (Taboola)","Local.com Appnexus","Zoomcorner.com - Taboola - PS","Discoverlocal.net (Taboola)","Elderlytimes.com (Taboola)","Yahoo.com Health Taboola","Yahoo.com Auto Taboola","Yahoo.com Telecom Taboola","Yahoo.com Tech Taboola","Yahoo.com CPG Taboola","Yahoo.com Travel Taboola","Yahoo.com Retail Taboola","Herstyle - DBM","SavingPanda.com (GEMINI)","SaverDeals.net (GEMINI)","2017Prices.com (Gemini)","Deals2017.net (Gemini)","Forbes.com (Gemini)","BuyNation.net (Gemini)","ShopDealio.co.uk (Gemini)","CompareMarts.com (Gemini)","SavingPanda.co.uk (Gemini)","FitnessMagazine.com (Gemini)","WebMD.com (GEMINI)","BHG ARB (Meredith) - Gemini","Findanswers.online gemini","Caloriebee.com (GEMINI)","hubpages (GEMINI)","Healdove.com (GEMINI)","Toughnickel.com (GEMINI)","CheapBargain.net (GEMINI)","2017Discounts.com (Gemini)","JeevesKnows (Gemini)","SaverPrices.net (GEMINI)","EmPowHer.com (GEMINI)","Yearend.Discount (Gemini)","ComparePoint.us (Gemini)","QualityHealth.com Gemini","Shoponym.com (Gemini)","PriceoCity.com (Gemini)","Dealatopia.com (Gemini)","Elderlytimes.com(Gemini)","gotquestion.com(GEMINI)Â ","Allanswers.com (GEMINI)","Health.online (Gemini)","Emedicinehealth.com (Gemini)","Medicinenet.com (Gemini)","Rxlist.com (Gemini)","Directory.site Gemini","Local.com Legacy Gemini","Local.com Gemini GY","Directory.site Gemini GY","Directory.site Legacy Gemini","MrLocal.com Gemini","Laptop.Blackfriday (Gemini)","KitchensDeals.com (Gemini)","TVSales.Blackfriday (Gemini)","BlackFridayUS.net (Gemini)","Mobiles.Blackfriday (Gemini)","Health.online Titanium (Gemini)","BlackFridayay.com (Gemini)","USABlackFriday.com (Gemini)","GameConsole.deals (Gemini)","Selectdeals.net(GEMINI)","Shop.Local.com (Gemini)","Local.com Gemini Native","CyberMondayay.com (Gemini)","BlackFridayElectronics.net (Gemini)","Discover.Guru (Gemini)","Jennystores.com(GEMINI)","DealsandSteals.com(GEMINI)","MobilesInsider.com (Gemini)","TheDiscount.Store (Gemini)","SpeakingHealth - Gemini","Beyondthewire.online - Gemini","Businessfocus.online - Gemini","Mensfocus.online - Gemini","Herstyle.online - Gemini","Shop.MrLocal.com Gemini","2018Prices.com (GEMINI)","2018Discounts.com (Gemini)","Deals2018.com (GEMINI)","TheDealGarden.com (Gemini)","Cardswallet.com (GEMINI)","EliteSavings.net (GEMINI)","TheHealthDiary.com (Gemini)","Wantbargain.com(GEMINI)","ShopperSphere.com(GEMINI)","TargetedDeals.com(GEMINI)","ExploreLocal.net Gemini","InnerSpark.online - Gemini","Shop.Explorelocal.net Gemini","Icananswerthat.com Gemini","HealthAccess.com Gemini","BuyStores.net (GEMINI)","DegreesCompared.com (GEMINI)","comparecards.online(GEMINI)","Topvarsity.com(GEMINI)","ThunderDeals.net Gemini","Cellphones.Guru Gemini","TheDiscountFarm.com Gemini","shop.Ziphip.com Gemini","Ziphip.com Gemini","MSN Lifestyle (GEMINI)","MSN Travel (GEMINI)","MSN Auto (GEMINI)","MSN Health (GEMINI)","MSN Entertainment (GEMINI)","MSN Sports (GEMINI)","MSN Food (GEMINI)","MSN Money (GEMINI)","TheBargains.Store Gemini","TheDealstream.com Gemini","Savings2018.com (GEMINI)","AllYear.deals (GEMINI)","Answerly.net Gemini","Supreme.Deals Gemini","TVsInsider (Gemini)","Sales365.net (GEMINI)","LaptopsInsider.com (Gemini)","FinderMonk.com (GEMINI)","Infoshoppe.net (Gemini)","FAQsLibrary.com(Gemini)","TrendsAndDeals.com Gemini","SeasonDeals.store(GEMINI)","HealthInsider.net Gemini","HealthAtoZ.net Gemini","Answeropedia.net Gemini","Mobilesarena.com Gemini","BlackFridayThunder.com Gemini","DiscountArena.net Gemini","Bargainopedia.com Gemini","TheDealArena.com Gemini","CarandDriver.com (Gemini)","CollegeArena.com Gemini","Discoverlocal.net Gemini","Bargainarena.net Gemini","ThinkOffers.net(Gemini)","BlackfridayArena.net Gemini","ShoppersArena.net Gemini","ResultsArena.com(Gemini)","CybermondayArena.com Gemini","Bargain.net Gemini","Offersportal.net(Gemini)","GadgetOffers.com(Gemini)","SmartDiscounts.net(Gemini)","Ultimatebargains.net Gemini","Compareopedia.com Gemini","Cybermondaypro.com Gemini","MrWiki.com Gemini","SeniorsInsider.net Gemini","FinanceNinja.net Gemini","BlackFridayMart.com Gemini","CyberMonday247.com (Gemini)","Mobiles247.net (Gemini)","USPhones.net (Gemini)","BlackFriyay.com (Gemini)","MyBlackFriday.net (Gemini)","USACyberMonday.com (Gemini)","All.Discount (Gemini)","Offers247.net (Gemini)","Bargains.blackfriday(Gemini)","BlackFridayGrabs.com (Gemini)","NowSales.net (Gemini)","Xmas.discount(Gemini)","BlackFridaySavings.com(Gemini)","CyberMondayUS.com (Gemini)","BlackFridayBargains.com(Gemini)","Topichomes.com(Gemini)","CyberMondayUsa.com(Gemini)","CyberMondaySales.net(Gemini)","BlackfridayStreet.com(Gemini)","BlackFridayFair.com(Gemini)","CyberMondayStreet.com(Gemini)","Dailybuffer.online - Gemini","HealthyFi.net (Gemini)","Find.Local.com Verizon","Top10Ratings.com Verizon","Speakinghealth - DBM","FAQsLibrary.com (DBM)","MobilesArena.com (DBM)","FAQs.com (DV 360)","Dev_Test_1 ","Dev_Test_2 ","demo","Forbes.com 01_navfox","Dev_Test_3 ","ComparePoint.us (BING)","Shoponym.com (BING)","DealsZone.co.uk (BING)","PriceoCity.com (BING)","ListScoop.com (BING)","2016Discounts.com (BING)","AllDiscounts.co.uk (BING)","Forbes.com (BING)","TheStreet.com - Bing","Dealatopia.com (BING)","HomeAndMore.us (BING)","More Arbitrage (Meredith) - BING","Recipe Arbitrage (Meredith) - BING","BHG ARB (Meredith) - BING","Parenting Arbitrage (Meredith) - BING","FamilyCircle Arbitrage (Meredith) - BING","EatingWell ARB (Meredith) - BING","DivineCaroline Arbitrage (Meredith) - BING","RachaelRayMag ARB (Meredith) - BING","FitnessMagazine ARB (Meredith) - BING","MidWestLiving ARB (Meredith) - BING","Parents Arbitrage (Meredith) - BING","CompareMarts.com (BING) Arbitrage","Compare.Online (EBAY) Bing Arbitrage","AllLocal.net Bing Arbitrage","CheapMunks.com (BING)","SpeakLocal.net (BING)","ShopDealio.net (BING)","SavingPanda.com (BING)","SaverDeals.net (BING)","Local.com Outbrain","Local.com Outbrain 2","Local.com Outbrain 3","TheHealthDiary.com (Outbrain) 101","Local.com Outbrain 4","GotQuestion.com(OUTBRAIN)","TheHealthDiary.com (Outbrain-2) 102","Local.com Outbrain 5","HealthAccess.com Outbrain 6","Allanswers.com (OUTBRAIN)","Speakinghealth.com - Outbrain","Theprettylife.online - Outbrain","Explorelocal.net (DM-bing)","Forbes.com AFS (BING)","theseniorguardian.com 01","Local Forbes BING","Food.Dailycluster - Bing","Theprettylife.online - Bing","MSN Health (BING)","MSN Auto (BING)","MSN Money (BING)","MSN Lifestyle (BING)","Laptopsinsider.com (BING)","TVsInsider.com (BING)","GenieSearch.net (BING MSAN)","InfoShoppe.net (BING MSAN)","MSN Entertainment (BING)","MSN Food (BING)","MSN Sports (BING)","MSN Travel (BING)","ResultsGator.com(BING)","USACyberMonday.com (Bing)","WebMD.com(BING-2)","FindAnswers.online(BING-2)","Hubpages.com(BING-2)","HealDove.com(BING-2)","ToughNickel.com(BING-2)","EmpowHer.com (Bing-Mnet)","QualityHealth.com (Bing-Mnet)","FamilyCircle.com (Bing-Mnet)","ResultsGator.com(BING-1)","ResultsGator.com(BING-2)","MSN Health (BING-1)","MSN Auto (BING-1)","MSN Sports (BING-1)","MSN Lifestyle (BING-1)","MSN Money (BING-1)","MSN Travel (BING-1)","MSN Entertainment (BING-1)","MSN Food (BING-1)","Autobytel.com BING 1","YouMeMindBody.com BING 1","HealthProAdvice.com BING 1","PatientsLounge.com BING 1","Deals2019.com (Bing)","MSN Websearch (Bing-1)","MSN Websearch (Bing-2)","MSN Websearch (Bing-3)","MSN Websearch (Bing-4)","MSN Websearch (Bing-EC-1)","ExpertMonks.com (Bing)","FinderCrew.net (Bing)","Aerlie.net (Bing)","Bruckie.com (Bing)","TheDiscountMill.com (Bing)","ThunderDeals.net Bing","Ziphip.com Bing","shop.Ziphip.com Bing","TheDealstream.com Bing","TheBargains.Store Bing","Supreme.Deals Bing","Answerly.net Bing","TrendsAndDeals.com Bing","HealthAtoZ.net Bing","Mobilesarena.com Bing","DiscountArena.net Bing","Bargainopedia.com Bing","TheDealArena.com Bing","CollegeArena.com Bing","BlackfridayArena.net Bing","CybermondayArena.com Bing","Compareopedia.com Bing","MrWiki.com Bing","FinanceNinja.net Bing","SeniorsInsider.net Bing","CyberMondaySales.net(Bing)","CybermondayFest.com(Bing)","BlackFridayBargains.com(Bing)","Bargains.blackfriday(Bing)","MobilesWizard.com(Bing)","Usedcarinsights.com (BING)","BlackFridayFair.com(Bing)","BlackfridayStreet.com(Bing)","CyberMondayStreet.com(Bing)","Investingbytes.com (BING)","Financeninja.net(BING)","Thebizcluster.com(BING)","Ultimate.careers(BING)","BusinessBytes.net(BING)","Localbytes.net (Bing)","Local.com Bing 2","Autobytel.com BING 2","Top10Ratings.com Anti-Snoring BING 2","HealthCare.com Bing","OneStopStore.net (Bing)","GetBestBargains.com (Bing)","DealsCart.net (Bing)","YouMeMindBody.com BING 2","PatientsLounge.com BING 2","HealthProAdvice.com BING 2","Empowher.com SEM (BING)","Pearlywhytes.com Bing","MSN Websearch (Bing-5)","MSN Websearch (Bing-6)","MSN Websearch (Bing-7)","MSN Websearch (Bing-EC)","Articles.Local.com Bing 2","Top10Ratings.com Website Building (Bing)","MSN Websearch (Bing-UK)","MSN Websearch (Bing-UKHT)","MSN Websearch (Bing-CA)","MSN Websearch (Bing-CAHT)","GadgetsOnSale.net (Bing)","GadgetArchives.com (Bing)","DailyShoppingTrends.com (Bing)","ShoppersScope.com (Bing)","Top10Ratings.com Dating (Bing)","MrLocal.com Bing 2","Top10Ratings.com VoIP (Bing)","Top10Ratings.com Website Hosting (Bing)","More.com (Meredith) Bing 2","Parenting.com (Meredith) Bing 2","FamilyCircle.com (Meredith) Bing 2","FitnessMagazine.com (Meredith) Bing 2","BHG.com (Meredith) Bing 2","EatingWell.com (Meredith) Bing 2","RachaelRayMag (Meredith) Bing 2","MidWestLiving.com (Meredith) Bing 2","Caloriebee.com Bing 2","TheStreet.com Bing 2","OneStopArena.com (Bing)","BackPackTales.com (Bing)","MSN Health ZC (Bing)","MSN Travel ZC (Bing)","MSN Lifestyle ZC (Bing)","MSN Money ZC (Bing)","MSN Auto ZC (Bing)","MSN Food ZC (Bing)","MindBodyGreen.com (Bing)","Top10Ratings.com Meal Delivery (Bing)","Resultsinsights.com(Bing)","Localinsights.net (Bing)","Shop.Local.com Bing New","Shop.MrLocal.com Bing New","DegreesCompared.com (BING)","BuyStores.net (BING)","Autoswire.com(BING)","Topvarsity.com(BING)","Savings2018.com (BING)","AllYear.deals (BING)","Sales365.net (BING)","FinderMonk.com (BING)","SeasonDeals.store(BING)","FAQsLibrary.com (BING)","CarandDriver.com (BING)","ThinkOffers.net (BING)","ResultsArena.com (BING)","Offersportal.net (BING)","SmartDiscounts.net (BING)","GadgetOffers.com (BING)","MyBlackFriday.net (Bing)","CyberMonday247.com (Bing)","AllBlackFriday.net (Bing)","BlackFriyay.com (Bing)","All.Discount (Bing)","10Offers.net (Bing)","Mobiles247.net (Bing)","USPhones.net (Bing)","NowSales.net (Bing)","CyberMondayUsa.com(BING)","BlackFridaySavings.com (BING)","Topichomes.com (BING)","Xmas.discount (BING)","CyberMondayOffers.com(Bing)","CyberMondayUS.com (Bing)","BlackFridayGrabs.com (Bing)","CyberMondayStore.net (Bing)","DealsPanda.net (Bing)","DealsBlackFriday.net (Bing)","BlackFridayGuru.net (Bing)","DealsCyberMonday.net (Bing)","DivaTrends.net (BING)","Immediatediscounts.com (BING)","CardsExpert.net(BING)","Healthpixie.net (Bing)","HealthyFi.net (Bing)","Healthorigins.com(Bing)","2019Discounts.com (Bing)","2019Prices.com (Bing)","Savings2019.com (Bing)","Superrecherche.com (Bing)","Suchdichgluecklich.com (Bing)","EthanStores.com (Bing)","InsightsAuto.com (Bing)","SeekNemo.com (Bing)","InfoSage.net (Bing)","QuestGuru.net (Bing)","SaversArena.com (Bing)","SavingsHub.net (Bing)","OfferSteals.com (Bing)","2019Offers.net (Bing)","2019Saver.net (Bing)","HealthMaester.com (Bing)","SavvyMonk.com (Bing)","FinderBee.net (Bing)","InfoPundit.net (Bing)","Millesdecouvertes.com (Bing)","Alleserdenkliche.com (Bing)","AnswersDepot.net (Bing)","Choixatoutva.com (Bing)","AutoArena.com (Bing)","OffersGarden.com (BING)","GenieSage.com (BING)","KnowNemo.com (BING)","SearchInstantly.net (BING)","GenieAdvice.com (BING)","InsightsPanda.com (BING)","DealsFarms.com (BING)","DealsDivine.com (BING)","OffersPod.com (BING)","ShoppersFest.com (BING)","ShoppersPoint.net (BING)","OffersLounge.com (BING)","Mortow.com (Bing)","Nobbyn.com (Bing)","Patteo.net (Bing)","Pazton.com (Bing)","Qualdo.net (Bing)","BlackFridayUKSale.com (Bing)","FRBlackFriday.com (Bing)","AgileFeed.net (Bing)","Averles.com (Bing)","Kipen.net (Bing)","Quelly.net (Bing)","BlackFridayPrecios.com (Bing)","ESCybermonday.com (Bing)","247BlackFridayDeals.com (Bing) ","BlackFridayDeal4U.com (Bing) ","BlackFridayPrices.net (Bing) ","CyberMondayMania.net (Bing)","CyberMondayPrices.com (Bing)","CyberMondayStores.net (Bing) ","UKBlackFridayDeals.com (Bing) ","UKCyberMonday.com (Bing) ","ESBlackFriday.com (Bing) ","OfertasCybermondayUS.com (Bing)","Resultsfeed.net (Bing)","Advicepool.net (Bing)","Swiftfeed.Net (Bing)","Cybermondaypicks.com (Bing)","BlackFridayFest.net (Bing)","Cybermondaycart.com (Bing)","Blackfridaypicks.net (Bing)","BlackFridayClub.net (Bing)","BlackFridayDepot.com (Bing)","CyberMondayDaily.com (Bing)","CyberMondayNow.com (Bing)","BlackFridayStar.net (Bing)","BlackFridaySteals.net (Bing)","CyberMonday365.com (Bing)","CyberMondayClub.net (Bing)","Resultspanda.com (Bing)","Resultsdigest.com (Bing)","CyberMonday247.com (Bing 2)","BlackFridayGrabs.com (Bing 2)","Discover.Guru (Bing)","TheDiscount.Store (Bing)","TheDealGarden.com (Bing)","Cellphones.Guru Bing","TheDiscountFarm.com Bing","Antivirusreviews.com Bing","Answeropedia.net Bing","HealthInsider.net Bing","BlackFridayThunder.com Bing","Discoverlocal.net Bing","Bargain.net Bing","ShoppersArena.net Bing","Bargainarena.net Bing","Cybermondaypro.com Bing","Ultimatebargains.net Bing","BlackFridayMart.com Bing","CybermondayRush.com Bing","Top10Ratings.com Anti-Snorring BING 1","GrabDiscounts.net (Bing)","GizmozArena.com (Bing)","GizmoExpert.net (Bing)","Swipewiki.com (Bing)","Localwizard.net (Bing)","Resultsbee.com (Bing)","TravellersGuide.net BING","CheapBargain.net Bing","BuyNation.net Bing","BlackFridayStore.us - Bing","Health.online BING","XmasDiscounts.net (BING)","Yearend.Discount (BING)","FindAnswers.Online (BING)","2017Prices.com (Bing)","Deals2017.net (Bing)","WebMD.com (BING)","Rxlist.com (BING)","Emedicinehealth.com (BING)","Medicinenet.com (BING)","EmPowHer.com (BING)","SaverPrices.net (BING)","hubpages (Bing)","Toughnickel.com (BING)","Caloriebee.com (BING)","Healdove.com (BING)","QualityHealth.com (Bing)","Elderlytimes.com (BING)","Directory.site Bing 1","Allanswers.com(BING)","gotquestion.com(BING)","Local.com Legacy Bing","Directory.site Legacy Bing","Directory.site Bing GY","Local.com Bing GY","MrLocal.com Bing 1","BlackFridayay.com (Bing)","Selectdeals.net(BING)","Shop.Local.com (Bing)","MobilesInsider.com (BING)","Dealsandsteals.com (BING)","Jennystores.com (BING)","Shop.MrLocal.com Bing","Local.com Bing Mobile","iCanAnswerThat.com Bing","Cardswallet.com (BING)","Comparecards.online (BING)","TargetedDeals.com(BING)","ShopperSphere.com(BING)","Wantbargain.com(BING)","ExploreLocal.net Bing","Thethriftydeals.com - Bing","Shop.ExploreLocal.net Bing","HealthAccess.com Bing","Jp.Local.com Bing","Thelushhouse - Bing","Investopoly - Bing","onlineshopvergleich.com - Bing","Ouidiscount.com - Bing","Articles.Local.com Bing 1","SpeakingHealth - Bing","Health.online-Titanium (Bing)","Businessfocus.online - Bing","Beyondthewire.online - Bing","Cartrends.online - Bing","HappyPaws.online - Bing Titanium","2017Discounts.com Bing","JeevesKnows.com Bing","USPriceDrops.com Bing","USDealSteals.com Bing","CheapestDeals.net Bing","GenieSearch.net (BING)","Usoffers.net Bing","BargainUS.net (Bing)","DealStores.net (Bing)","ShopperDen.com (Bing)","Christmas.net (Bing)","BlackFridayElectronics.net (Bing)","USABlackFriday.com (Bing)","BlackFridayUS.net (Bing)","Mobiles.Blackfriday (Bing)","Laptop.Blackfriday (Bing)","TVSales.Blackfriday (Bing)","Bargain.net (Bing)","KitchensDeals.com (Bing)","GameConsole.Deals (Bing)","CyberMondayay.com(Bing)","BlackFridayShoppe.net(Bing)","2018Prices.com (BING)","2018Discounts.com (BING)","Deals2018.com (BING)","Offers247.net (Bing)","InfoInsider.net (BING)","InfoShoppe.net (BING)","KnowHubb.com (BING)","DailyCluster.com - Bing","EliteSavings.net (BING)","MrLocal.com Outbrain 7","ExploreLocal.net Outbrain 8","Speakinghealth Expansion Outbrain","HappyPaws Traditional Outbrain","HappyPaws Expansion Outbrain","Herstyle Expansion Outbrain","Herstyle Traditional Outbrain","Empowher Expansion Outbrain","Empowher Traditional Outbrain","InfoShoppe (OutBrain All) - 103","InfoShoppe (OutBrain Whitelist) - 104","Investopoly - Outbrain","DailyCluster - Outbrain","Thelushhouse - Outbrain","Thehealthcluster - Outbrain","Cartrends.online - Outbrain","Thebizcluster - Outbrain","Answeropedia.net Outbrain 9","Healthinsider.net Outbrain 10","Crawlerwatch Expansion Outbrain","Crawlerwatch Traditional Outbrain","Healthpixie.net (OutBrain) 105","InfoInsider.net (OutBrain) 106","SearchInsider.net (OutBrain) 107","KnowHubb.com (OutBrain) 108","FinderMonk.com (OutBrain) 109","DealsBlackFriday.net (OutBrain) 110","CyberMondayUS.com (OutBrain) 111","TheDealStream.com Outbrain 20","TheDiscountfarm.com Outbrain 18","DiscountArena.net Outbrain 19","Blackfridayarena.net Outbrain 11","BlackFridayMart.com Outbrain 12","Supreme.Deals Outbrain 13","TheDealGarden.com Outbrain 14","Bargainopedia.com Outbrain 15","Discoverlocal.net Outbrain 16","Ziphip.com Outbrain 17","Raiseyourstakes-Expansion Outbrain","BlackFridayFair.com(OUTBRAIN)","CyberMondayStreet.com(OUTBRAIN)","HealthCare Traditional Outbrain","Health.Online Traditional Outbrain","Local.com Outbrain 21","Local.com Outbrain 22","Local.com Outbrain 23","Local.com Outbrain 24","Local.com Outbrain 25","Elitesavings.net (OUTBRAIN)","Comparecards.online (OUTBRAIN)","Travellersguide.net (OUTBRAIN)","Thebizcluster.com (OUTBRAIN)","businessbytes.net (OUTBRAIN)","Investingbytes.com (OUTBRAIN)","Local.com Outbrain 26","Local.com Outbrain 27","Local.com Outbrain 28","Local.com Outbrain 29","Local.com Outbrain 30","ShopperDen.com (OutBrain) 113","SaversArena.com (OutBrain) 112","Local.com Outbrain 31","Top10Ratings.com Outbrain","HealthyFi.net (OutBrain) 114","SearchAnswers.net (Outbrain - Sedo)","2019Discounts.com (Outbrain) 115","2019Prices.com (Outbrain) 116","Local.com Outbrain MSN","Local.com Outbrain MSN 2","Local.com Outbrain MSN 3","Local.com Outbrain MSN 4","Deals2019.com (Outbrain) 117","DealsFarms.com (Outbrain) 118","GenieSage.com (Outbrain) 119","KnowNemo.com (Outbrain) 120","OffersLounge.com (Outbrain) 121","SearchInstantly.net (Outbrain) 122","ShoppersPoint.net (Outbrain) 123","DealsDivine.com (Outbrain) 124","GenieAdvice.com (Outbrain) 125","InsightsPanda.com (Outbrain) 126","OffersGarden.com (Outbrain) 127","OffersPod.com (Outbrain) 128","ShoppersFest.com (Outbrain) 129","MSN Websearch (Outbrain -1)","MSN Websearch (Outbrain -2)","MSN Websearch (Outbrain -3)","MSN Websearch (Outbrain -4)","Yahoo.com Outbrain 1","Yahoo.com Outbrain 2","Yahoo.com Outbrain 3","Yahoo.com Outbrain 4","Yahoo.com Outbrain 5","Yahoo.com Outbrain 6","FAQsLibrary.com (Outbrain)","Elderlytimes.com (Outbrain)","Health.Online - Taboola","AllLocal.net Display & Remarketing","BuyCentral.us-DISP-USA","ShoppersPoint.net (Display)","CheapBargain.net (DISPLAY)","2019Prices.com (Search)","Yahoo.com Real Estate Display","USPriceDrops.com (Search)","Yahoo.com Display","Local.com Adwords Mnet MCC","EthanStores.com (Display)","DegreesCompared.com (SEARCH)","Yahoo.com Technology Display","hubpages (SEARCH)","Allanswers.com(Display)","2019Saver.net (Display)","BuscarHoy.net (Display)","MSN Auto (Search-1)","Pazton.com (Display)","CyberMondayStreet.com (Display)","MobilesArena.com Search","BlackFridayDealsUK.net(Search)","Health.online (SEARCH)","InfoSavant.net (Display)","ComparePoint.us Display","Cardswallet.com (SEARCH)","BlackFridayMart.com Search","CarAndDriver.com (SEARCH)","OneStopStore.net (Display)","MSN Websearch ROO (Search-4)","NowSales.net (Display)","Ultimatebargains.net Display","Qualdo.net (Search)","Alleserdenkliche.com (Display)","Millesdecouvertes.com (Display)","Savings2018.com (SEARCH)","TheDealStream.com (Search)","Dealsandsteals.com (SEARCH)","CyberMondayay.com (Search)","MindBodyGreen.com (Display)","Bargain.net (Search)","Cybermondayrush.com. Search","MSN Websearch (Search-UKHT)","DescubreHoy.net (Search)","Dealstrooper.com-Google","CyberMonday247.com (Search)","AnswersDepot.net (Search)","KnowNemo.com (Search)","FRBlackFriday.com (Search)","Autoswire.com(SEARCH)","GenieAdvice.com (Search)","BusinessBytes.net(Display)","Allanswers.com(SEARCH)","CheapBargain.net (SEARCH)","HealthCare.com Search","BlackFriyay.com (Display)","Deals123.co.uk-DISP-UK","CyberMondayay.com (Display)","Cardswallet.com(DISPLAY)","AllBlackFriday.net (Search)","Xmas.discount(Search)","DealFinders.net-DISP-USA","Offersportal.net(Search)","InsightsPanda.com (Display)","OneStopArena.com (Search)","OffersGarden.com (Search)","DealsPanda.net (Search)","CyberMondayUsa.com(SEARCH)","DealsDivine.com (Display)","BlackFridayDepot.com (Search)","BuyStores.net (SEARCH)","InfoInsider.net (Display)","MSN Entertainment (Search-1)","Superrecherche.com (Display)","Usedcarinsights.com(Search)","CyberMondayClub.net (Display)","BlackfridayArena.net Display","Discoverlocal.net Search","Kipen.net (Search)","MSN Health (Search-1)","PatientsLounge.com Search","Directory.site Adwords Mobile","TheDealStream.com (Display)","MuchasCosas.net (Display)","Forbes AFS","BlackFridayPrecios.com (Display)","OptimumAutoGY_1RKW_MN1","CyberMondayStores.net (Search)","2016Discounts.com Search","Toughnickel.com(Search)","CybermondayArena.com Search","Savings2019.com (Display)","Discoverlocal.net Display","MobilesInsider.com (Search)","CybermondayArena.com Display","2018Prices.com (SEARCH)","Yahoo.com Telecom Display","AnswersDepot.net (Display)","Bargains.blackfriday(Search)","Topvarsity.com(DISPLAY)","Cybermondaycart.com (Display)","Advicepool.net (Display)","AllBlackFriday.net (Display)","FinderMonk.com (SEARCH)","AutoArena.com (Display)","Knowhubb.com (SEARCH)","MobilesWizard.com(Search)","MSN Websearch (Search-EC)","Deals123-SRCH-USA","MSN Websearch ROO (Search-2)","EmPowHer.com (SEARCH)","UKBlackFridayDeals.com (Search) ","Healthoholic - Google - GM","MSN Travel (Search)","Choixatoutva.com (Display)","Antivirusreviews.com Search","Pearlywhytes.com Search","Forbes.com SEARCH","Yahoo.com Travel Display","MrLocal.com Display","BlackFridayClub.net (Search)","InformationGY_1RKW_MN1","DiscountArena.net Search","Directory.site Re-Marketing","Bargainopedia.com Display","AllYear.Deals(Display)","Recipe.com-SRCH-(Meredith)","InfoInsider.net (SEARCH)","MSN Websearch (Search-2)","Top10Ratings.com Meal Delivery (Search)","MobilesArena.com Display","BlackFridayGrabs.com (Display)","QuestGuru.net (Display)","Advicepool.net (Search)","Superrecherche.com (Search)","Cybermondaypicks.com (Search)","Localinsights.net (Search)","Swipewiki.com (Display)","DealStores.net Search","USDealSteals.com (Display-2)","Priceocity.com Search","Yahoo.com Auto Display","10Offers.net (Search)","HealthCare.com Display","CyberMonday247.com (Display)","CollegeArena.com Search","Supreme.Deals Search","Patteo.net (Search)","ShopperSphere.com(DISPLAY)","MyBlackFriday.net (Search)","CyberMondayUS.com (Display)","DealsCart.net (Search)","OfertasCybermondayUS.com (Search)","Aerlie.net (Display)","HealthProAdvice.com Search","Bargain.net Search","BlackFridayDeal4U.com (Display)","2017Prices.com (Display)","LaptopsInsider.com (Display)","SeekNemo.com (Display)","2016Discounts.com Display","Ultimatebargains.net Search","FitnessMagazine.com-SRCH-(Meredith)","SpeakLocal.net Search","InfoSage.net (Display)","ShoppersScope.com (Display)","SaverPrices.net (Display)","Parenting.com-SRCH-(Meredith)","CarAndDriver.com (DISPLAY)","AgileFeed.net (Search)","MSN Auto (Search)","JeevesKnows.com (Search)","TheDealGarden.com Display","GenieSage.com (Search)","Cellphones.guru Display","ShoppersFest.com (Search)","2019Discounts.com (Search)","ExpertMonks.com (Display)","Antivirusreviews.com Display","BuscarHoy.net (SEARCH)","Emedicinehealth.com (SEARCH)","MidWestLiving.com-SRCH-(Meredith)","UKCyberMonday.com (Search)","Compareopedia.com Display","Selectdeals.net(DISPLAY)","Deals2018.com (2-Display)","Swiftfeed.Net (Search)","Local.com Forbes 2","FindAnswers.Online(Display)","Bargain.net Display","Local Forbes","Mobiles247.net (Display)","CyberMondaySales.net(SEARCH)","BargainUS.net Display","2017Prices.com (Search)","Compareopedia.com Search","All.Discount (Search)","GizmoExpert.net (Search)","Laptop.BlackFriday (Search)","Medicinenet.com (SEARCH)","BlackFridayGrabs.com (Search-2)","BlackFridayPrices.net (Display) ","USPriceDrops.com (Display)","Blackfridaypicks.net (Search)","Mobiles.BlackFriday (Search)","TVsInsider.com (SEARCH)","OffersPod.com (Search)","Nobbyn.com (Display)","SavvyMonk.com (Search)","BlackFridayGuru.net (Search)","Cybermondaypro.com Display","Directory.site S2S","2019Discounts.com (Display)","Speakinghealth - Google","Yahoo.com Health Display","MSN Websearch ROO (Search)","247BlackFridayDeals.com (Search) ","Top10Ratings.com VOIP (Search)","OffersLounge.com (Display)","Nobbyn.com (Search)","HomeAndMore.us Display","HealthMaester.com (Display)","BHG.com-SRCH-(Meredith)","GadgetArchives.com (Display)","Kipen.net (Display)","Dev-Test","Savings2019.com (Search)","Bargainarena.net Search","BlackFridaySteals.net (Search)","CyberMonday365.com (Search)","MSN Travel (DISPLAY)","2018Discounts.com (SEARCH)","Savings2018.com (Display)","LaptopsInsider.com (SEARCH)","CompareMarts.com (SEARCH)","DailyShoppingTrends.com (Display)","Alleserdenkliche.com (Search)","Suchdichgluecklich.com (Search)","MSN Websearch (Search-UK)","BackPackTales.com (Display)","MSN Websearch ROO (Search-6)","Local.com Forbes","TheThriftyDeals.com (Display-3)","ThinkOffers.net(Search)","DegreeSleuth.com (SEARCH)","GadgetArchives.com (Search)","SaverDeals.net Display","Qualdo.net (Display)","ShoppersScope.com (Search)","CyberMondayOferta.com (Search)","AllDiscounts.co.uk Search","Millesdecouvertes.com (Search)","InsightsPanda.com (Search)","elbuscador.net (Display)","BlackFridayElectronics.net (Search)","InfoPundit.net (Display)","Offers247.net (Search)","MobilesWizard.com(Display)","BlackFridayStar.net (Search)","Dealsandsteals.com (DISPLAY)","USPhones.net (Display)","GeniuSearch.co.uk (SEARCH)","MuchasCosas.net (Search)","UKCyberMonday.com (Display) ","SaversArena.com (SEARCH)","ESBlackFriday.com (Display) ","2019Saver.net (Search)","ElectronicsInsider.net (Search)","Top10Ratings.com Dating (Search)","GeniuSearch.co.uk (Display)","EthanStores.com (Search)","MSN Money (Search-1)","MSN Websearch (Search-4)","Dealatopia.com Display","BlackFridayHub.net (Search)","Resultsdigest.com (Display)","Immediatediscounts.com(Display)","AgileFeed.net (Display)","GadgetOffers.com(Search)","HealthMaester.com (Search)","Techdealsforyou.net (Display)","LiveRetailTherapy.com - Google","CyberMondayPrices.com (Search) ","Discover.Guru Display","FindADeal.us-DISP-USA","MSN Lifestyle (Search-1)","FAQsLibrary.com (Display)","Blackfridaybargains.com(Display)","ComparePoint.us Search","Dailybuffer - Google","theseniorguardianGY_1RKW_MN1","MSN Health (DISPLAY)","Localwizard.net (Display)","FinderCrew.net (Search)","CyberMondayZone.com (Search)","USDealSteals.com (SEARCH)","Jennystores.com (SEARCH)","BlackfridayArena.net Search","BlackFridayUS.net (Display)","DegreesCompared.com (Display)","elbuscador.net (Search)","USOffers.net Display","Shoponym.com-SRCH-USA","Mortow.com (Search)","SearchInstantly.net (Display)","GadgetsOnSale.net (Display)","247BlackFridayDeals.com (Display)","ESCybermonday.com (Search)","MSN Websearch ROO (Search-3)","InfoSavant.net (Search)","Techdealsforyou.net (Display-2)","FinderMonk.com (Display)","Deals2018.com (SEARCH)","ElectronicsInsider.net (Display)","QuestGuru.net (SEARCH)","Pearlywhytes.com Display","Deals2019.com (Search)","GetBestBargains.com (Display)","BlackFridayShoppe.net (Search)","SaversArena.com (Display)","Discover.Guru (Search)","CybermondayFest.com (Display)","Wantbargain.com(DISPLAY)","BlackFriyay.com (Search)","MrLocal.com Adwords","Crawlerwatch 2 - Google","Jennystores.com(DISPLAY)","MSN Lifestyle (Search)","InfoPundit.net (Search)","Listscoop-DISP-USA","HomeGardenHub.net (SEARCH)","Directory.site Display","Supreme.Deals Display","Resultsinsights.com(Display)","2018Prices.com (Display)","CyberMondayMania.net (Display)","CheapestDeals.net (Search)","Patteo.net (Display)","ThunderDeals.net Search","BusinessBytes.net(SEARCH)","BlackfridayStreet.com (Display)","MSN Websearch (Search-CAHT)","BuyNation.net Search","Immediatediscounts.com(Search)","MSN Money (Search)","GrabDiscounts.net (Display)","Parents.com-SRCH-(Meredith)","DealsBlackFriday.net (Display)","GetBestBargains.com (Search)","Forbes.com DISPLAY","Top10Ratings.com Website Hosting (Search)","BlackFridayHub.net (Display)","DealsFarms.com (Display)","2017Discounts.com (Display)","Top10Ratings.com (SEARCH)","GenieSearch.net Search","CheapMunks.com Display","Caloriebee.com (SEARCH)","Loveofretail.net - Google - MS","FinderBee.net (Search)","Yahoo.com CPG Display","YearEnd.Discount (Display)","ShopDealio.co.uk (Search)","InfoDigest.co.uk Display","Cellphones.Guru Search","Ziphip.com Display","TheDealGarden.com","ShopDealio.net Display","MSN Websearch ROO (Search-1)","USPhones.net (Search)","Wellagic.com - Google - MS","DiscountArena.net Display","SeniorsInsider.net Display","CyberMondayOffers.com (Search)","TheThriftyDeals.com (Search)","ExpertMonks.com (Search)","2019Offers.net (Display)","focusedbiz.com","Autoswire.com(DISPLAY)","Findanswers.online(SEARCH)","Averles.com (Display)","10Offers.net (Display)","Top10Antivirusratings.com Search","Deals2017.net (Search)","CyberMondayStores.net (Display)","MSN Websearch ROO (Search-7)","KnowNemo.com (Display)","DealsPanda.net (Display)","CyberMondayDaily.com (Display)","CyberMonday247.com (Search-2)","SearchInsider.net (Display)","Mortow.com (Display)","CollegeArena.com Display","MSN Food (Search)","BlackFridayShoppe.net (Display)","SavingPanda.com Display","CompareMarts.com (DISPLAY)","ultimate.careers(Display)","Averles.com (Search)","Top10Ratings.com Anti-snoring (Search)","BuyCentral.us-SRCH-USA","USACyberMonday.com (Search)","SeekNemo.com (Search)","AllYear.Deals(SEARCH)","DealsFarms.com (Search)","Topvarsity.com(SEARCH)","DealFinders.net-SRCH-USA","BlackFridayUKSale.com (Display)","Techdealsforyou.net (Display-3)","Resultspanda.com (Search)","InfoSage.net (SEARCH)","Localwizard.net (Search)","DescubreHoy.net (Display)","Resultspanda.com (Display)","Listscoop-SRCH-USA","ThunderDeals.net Display","MSN Entertainment (DISPLAY)","GizmozArena.com (Display)","TheInfoDigest.com Search","Dealyscoop.com-Google","Deals2018.com (Display)","USDealSteals.com (Display)","SearchInsider.net (Search)","BlackFridayDeal4U.com (Search)","Sales365.net (Display)","BuyStores.net (Display)","Christmas.net (Search)","Yahoo.com Retail Display","Usedcarinsights.com(Display)","TargetedDeals.com(SEARCH)","2018Discounts.com (Display)","MSN Entertainment (Search)","MSN Auto (DISPLAY)","CyberMondayDaily.com (Search)","Naturalremedees - Google","ESBlackFriday.com (Search) ","AllLocal.net Search","MSN Sports (DISPLAY)","SavvyMonk.com (Display)","BlackFridayStar.net (Display)","BlackfridayStreet.com (Search)","Shoponym.co.uk Display","CyberMondayMania.net (Search)","Directory.site Legacy Adwords","Mobiles.Blackfriday (Display)","Resultsbee.com (Search)","EatingWell.com-SRCH-(Meredith)","ultimate.careers(SEARCH)","Bruckie.com (Display)","Bargainarena.net Display","GrabDiscounts.net (Search)","BlackFridayDepot.com (Display)","BlackFridaySteals.net (Display)","Offers247.net (Display)","More.com-SRCH-(Meredith)","Comparecards.online(Display)","Deals123-DISP-USA","QualityHealth.com (Display)","FinderBee.net (Display)","DivaTrends.net(Search)","ShoppersFest.com (Display)","Pazton.com (Search)","BlackFridayOferta.com (Display)","BlackFridaySavings.com(Display)","CyberMondayNow.com (Search)","InsightsAuto.com (Search)","Shoppersarena.net Search","SavingPanda.com Search","Shoppersarena.net Display","ShopperSphere.com(SEARCH)","OneStopArena.com (Display)","Shopdealio.co.uk (Display)","InfoDigest.co.uk Search","Resultsfeed.net (Display)","BlackFridayGrabs.com (Search)","Elderlytimes.com(SEARCH)","GadgetsOnSale.net (Search)","Top10Ratings.com Website Builders (Search)","TVsInsider.com (Display)","Wellseries - Google","Healthtonic.net - Google","Aerlie.net (Search)","Choixatoutva.com (Search)","Getmoredaily.com - Google","FindAnswers.online Taken","BargainUS.net Search","DailyShoppingTrends.com (Search)","CheapestDeals.net (Display)","FinderCrew.net (Display)","Healdove.com(Search)","Uberscene.com - Google","CyberMondaySales.net(Display)","Resultsbee.com (Display)","MSN Websearch (Search-CA)","Localinsights.net (Display)","Priceocity.com Display","Elderlytimes.com(DISPLAY)","Knowhubb.com (Display)","OfertasCybermondayUS.com (Display)","CyberMondayUsa.com(DISPLAY)","SpeakLocal.net Display","ShopperDen.com (Search)","Blackfridaybargains.com(Search)","MSN Food (Search-1)","Resultsfeed.net (Search)","BlackFridayFair.com(Search)","MSN Websearch (Search-3)","Resultsdigest.com (Search)","GameConsole.Deals (Search)","Investingbytes.com(Search)","2019Offers.net (Search)","CyberMondayNow.com (Display)","BlackFridayUS.net (Search)","Bargainopedia.com Search","FindADeal.us-SRCH-USA","TheStreet.com-SRCH","AllDiscounts.co.uk Display","DealsCart.net (Display)","XmasDiscount.net (SEARCH)","ESCybermonday.com (Display)","Top10Antivirusratings.com Display","SelectDeals.net (SEARCH)","InnerSpark.online - HM - Google","DivaTrends.net(Display)","TheDiscount.Store Display","Yearend.Discount (SEARCH)","CyberMondayClub.net (Search)","ChristmasOffers.us Search","NowSales.net (Search)","Deals2019.com (Display)","Bruckie.com (Search)","Bargains.blackfriday(Display)","SearchInstantly.net (Search)","BackPackTales.com (Search)","2019Prices.com (Display)","Rxlist.com (SEARCH)","CyberMondayUS.com (Search)","CheapBargain.net (Display-2)","Blackfridaypicks.net (Display)","BlackFridayFair.com(Display)","AutoArena.com (Search)","TargetedDeals.com(DISPLAY)","MSN Health (Search)","OneStopStore.net (Search)","Cybermondaycart.com (Search)","TheDealarena.com Search","GizmozArena.com (Search)","CheapMunks.com (SEARCH) Arbitrage","BlackFridayay.com (Display)","MSN Food (DISPLAY)","MSN Travel (Search-1)","Autobytel.com (Display)","SavingsHub.net (Search)","MobilesInsider.com (Display)","MSN Websearch (Search-1)","TheDiscountMill.com (Search)","Top10Ratings.com (Display)","CyberMonday365.com (Display)","DealsZone.co.uk Display","BlackFridayThunder.com Search","TheDealarena.com Display","Mobiles247.net (SEARCH)","Investingbytes.com(Display)","Hertempo.com - Google - GM","BlackFridaySavings.com(Search)","MrWiki.com Search","CyberMondayOffers.com (Display)","RachaelRayMag.com-SRCH-(Meredith)","Crawlerwatch.com - Google","Cybermondaypicks.com (Display)","SeniorsInsider.net Search","gotquestion.com(SEARCH)","Zillopedia - Google","KitchensDeals.com (Search)","UKDealSteals.com Search","GenieSearch.net Display","CyberMondayPrices.com (Display) ","ForbesGY_1RKW_MN1","BlackFridayElectronics.net (Display)","XmasDiscounts.net (Display)","Wantbargain.com(SEARCH)","DealsZone.co.uk Search","All.Discount (Display)","BlackFridayMart.com Display","InsightsAuto.com (Display)","Ziphip.com Search","BlackFridayFest.net (Display)","2017Discounts.com (SEARCH)","Deals123.co.uk-SRCH-UK","OfferSteals.com (SEARCH)","TheThriftyDeals.com (Display-2)","ShoppersPoint.net (Search)","BlackFridayPrices.net (Search)","CyberMondayStore.net (Display)","GizmoExpert.net (Display)","Sales365.net (SEARCH)","MSN Sports (Search","InfoShoppe.net (Display)","Directory.site Adwords GY","DealStores.net Display","BlackFridayClub.net (Display)","MindBodyGreen.com (Search)","QualityHealth.com (SEARCH)","Quelly.net (Display)","GenieAdvice.com (Display)","BlackFridayUKSale.com (Search)","Shoponym.com-DISP-USA","GenieSage.com (Display)","SaverDeals.net Search","USABlackFriday.com (Display)","OffersPod.com (Display)","Localbytes.net (Display)","TheThriftyDeals.com (Display)","BlackFridayFest.net (Search)","FAQsLibrary.com(SEARCH)","SmartDiscounts.net(Display)","MSN Lifestyle (DISPLAY)","BlackFridayThunder.com Display","Resultsinsights.com(Search)","Quelly.net (Search)","CybermondayRush.com Display","TheInfoDigest.com Display","CybermondayFest.com (Search)","Comparecards.online(Search)","BuyNation.net Display","MSN Websearch ROO (Search-5)","OfferSteals.com (Display)","FRBlackFriday.com (Display)","USABlackFriday.com (Search)","OffersGarden.com (Display)","BlackFridayay.com (Search)","CyberMondayStreet.com (Search)","OffersLounge.com (Search)","Localbytes.net (Search)","CyberMondayStore.net (Search)","DegreeSleuth.com (Display)","Suchdichgluecklich.com (Display)","Retailholics.com - Google","UKDealSteals.com Display","EmpowHer.com (DISPLAY)","Dealatopia.com Search","MrWiki.com Display","TheDiscountMill.com (Display)","Swiftfeed.Net (Display)","ThinkOffers.net(Display)","ShopDealio.net Search","JeevesKnows (Display)","DealsBlackFriday.net (Search)","BlackFridayPrecios.com (Search)","Autobytel.com (SEARCH)","FamilyCircle.com-SRCH-(Meredith)","InfoShoppe.net (SEARCH)","Shoponym.co.uk Search","WebMD.com (SEARCH)","GadgetOffers.com(Display)","DealsDivine.com (Search)","OffersGarden.com (Search2)","CyberMondayUK.net(Search)","YouMeMindBody.com Search","Seasondeals.store(Search)","Swipewiki.com (Search)","MSN Sports (Search-1)","Compare.online - SRCH","SmartDiscounts.net(Search)","MSN Money (DISPLAY)","Livebtter.com - Google","Techdealsforyou.net (Search)","UsOffers.net Search","TheDiscount.store (Search)","Offersportal.net(Display)","DivineCaroline.com-SRCH-(Meredith)","HomeAndMore.us Search","DealsCyberMonday.net (Search)","TVSales.BlackFriday (Search)","Seasondeals.store(DISPLAY)","UKBlackFridayDeals.com (Display)","SavingsHub.net (Display)","BlackFridayOferta.com (Search)","SaverPrices.net (Search)","FAQs.com (Pinterest)","HealthyFi.net (Pinterest)","Health.online (TEST)","Health.online - Site","Cybermonday247.com (Facebook)","Theprettylife - 2 - Facebook","CyberMondayUSA.com(Facebook-2)","Health.Online 6 - FB - PB","Mobilesarena.com Facebook","HappyPaws.online - FB - HM","blackfridaydealsforyou - FB - 2","FindAnswers.Online(Facebook)","HealthPixie.net (Facebook)","Health.online - Facebook (OLD Deactivated)","Cartrends.online - Facebook","Beyondthewire.online - FB","Businessfocus.online - FB","HappyPaws.online - FB","HealthCarePanda - FB","KnowHubb.com (Facebook)","Cybermondayoffers.com(Facebook)","Top10Ratings.com Facebook","XmasDiscounts.net (Facebook)","DailyBuffer.Online 3 - FB - PB","MrWiki.com Facebook","Allanswers.com(Facebook)","Firstup.co(Facebook)","MobilesWizard.com (Facebook - 2)","Dailybuffer.online - FB","Tipsforhealth.com(Facebook)","Crawlerwatch - FB","MSN Sports (Facebook)","Investopoly - Facebook","HealthOrigins.com (Facebook-3)","BusinessBytes.net(Facebook)","WeThinkHealth.online 2 - FB - NM","Answeropedia.net Facebook","Allanswers.com-B&I (Facebook)","Healthorigins.com(Facebook)","HealthyFi.net (Facebook)","GizmozArena.com (Facebook)","Autobytel - Facebook","BuscarHoy.net (Facebook)","Thebizcluster - FB","FAQs.com (Facebook-2)","Seniorsinsider.net Facebook","WellCanvas 2 - FB - PS","Tipsforhealth.com(Facebook-2)","HealthCareToday.net (Facebook-2)","EthanStores.com (Facebook)","JennyStores.com (Facebook)","Academida.com - FB - PB","Bargains.blackfriday(Facebook)","Allanswers.com(Facebook-2)","HealthPixie.net (Three-Facebook)","MSN Money (Facebook)","Welldigest.com - FB - PB","Healthhandbook.online 4 - FB - PB","Offersportal.net (Facebook)","GadgetsOnSale.net (Facebook)","blackfridaydealsforyou - FB","MSN Lifestyle (Facebook)","GrabDiscounts.net (Facebook)","GetBestBargains.com (Facebook)","USPhones.net(Facebook)","Besthealthupdates - 2 - Facebook","Divatrends.net (Facebook)","DailyclusterNew1 - FB","HealthyFi.net (Facebook-4)","Uphealthnewz.com - FB - HM","FAQs.com (Facebook-8)","CyberMondayUS.com (Facebook)","MindBodyGreen - FB - MT","Searchanswers.net (Facebook - Tonic)","Icananswerthat.com(Facebook)","Healthaccess.com Facebook","WellnessGuru.net (Facebook-3)","MSN Food (Facebook)","OneStopArena.com (Facebook)","MSN Auto (Facebook)","Healthhandbook.online 3 - FB - PB","Inquirily.com 3 - FB - MT","HealthOrigins.com(Facebook - 2)","Academida.com 3 - FB - GM","Empirewell.com - FB - PB","ThinkOffers.net (Facebook)","Elderlytimes.com-Finance(Facebook)","IcanAnswerthat.com Facebook 2","BackPackTales.com (Facebook)","FeelingWell.online - FB - PB","Mranswerable.com (Facebook - Sedo)","FAQs.com (Facebook-10)","FAQs.com (Facebook-1)","Yahoo.com DR 2 Facebook","DailyShoppingTrends.com (Facebook)","HealthyFi.net (Facebook-3)","Allanswers.com-Finance(Facebook)","BlackFridayPrices.net (Facebook)","CybermondayFest.com (Facebook)","Searchanswers.net (Facebook - Sedo)","AmazeDeals.com (Facebook - Sedo)","Shoppingtherapy.online - FB - MT","Getmoredaily.com - FB - GM","Hertempo.com - FB - GM","Yahoo.com Technology Facebook","CybermondayArena.com (Facebook)","Uberscene.com - FB - MS","TrendingFAQS.com ONE (Facebook)","Investingbytes.com (Facebook)","HealthMaester.com (Facebook)","Healthnacity.com - FB - HM","FAQs.com (Facebook-3)","BlackFridayHub.net (Facebook)","247 Tempo - FB - MT","Zoobyte.com - FB - MT","MindBodyGreen - FB - MS","Find.Local.com Facebook","Dealrings FB - 1","Dealrings FB - 2","Wellagic.com - FB - MS","FAQs.com (Facebook-6)","Herstyle 3 - FB - GM","Hubwish.com - FB - PB","HealthyFi.net (2-Facebook)","Bargain.net Facebook","HealthNeeds.net (Facebook-2)","Glowwise.com - FB - PB","Yahoo.com Telecom Facebook","MobilesInsider.com (Facebook)","Zoomcorner.com - FB - PS","WebMd Titanium - FB","Healthcare.com 4 - FB - MT","Healthcare.com 3 - FB - HM","Pearlywhytes.com - Facebook","Empowher.com - FB","Empowher.com 2 - FB - PB","SearchInsider.net (Facebook)","Yahoo.com Travel Facebook","Yahoo.com Health Facebook","HealthMaester.com (Facebook-4)","Healthcare.com 2 - FB","Cybermondaypro.com (Facebook)","FAQs.com (Facebook-5)","ThePrettyLife - FB","Yahoo.com Auto Facebook","AllAnswers.com (SEM - FB)","Zillopedia.com 2 - FB - HM","TrendingFAQS.com TWO (Facebook)","MobilesWizard.com(Facebook)","TheHealthDiary.com (FB) - 2","Inquirily.com 2 - FB - NM","HealthInsider.net (Facebook-2)","Dealsmate 2 - FB","Healthhandbook.online - FB - PB","Livebtter.com - FB","247WallSt - FB- MT","DailyBuffer.Online 4 - FB - PB","BeyondTheWire - FB - PB","Mobiles.blackfriday (Facebook)","WeThinkHealth.online 3 - FB - NM","CyberMondayMania.net (Facebook)","ShoppersScope.com (Facebook)","CyberMondaySales.net (Facebook)","HealthMaester.com (Facebook-3)","Naturefrsh.com - FB - MT","CyberMondayOffers.com(Facebook-2)","Answerly.net (Facebook)","Dailycluster.com - FB","Dailytrend.online - FB - MT","Nurishedhealth.com - FB - MT","Findanswers.online(Facebook-2)","Bestofwell.com - FB - PB","FAQs.com (Facebook-7)","InsightsPanda.com (Facebook)","CrawlerWatch 2 - FB - MT","MindBodyGreen - FB - GM","Yahoo.com Facebook","HealthySquad.net (Facebook-2)","DailyBuffer.Online 2 - FB - ED","OneStopStore.net (Facebook)","Healthcare.com - FB","DealsCart.net (Facebook)","Mundo.com - FB - PB","Wellseries.com - FB - GM","Ajay Dev test","MSN Health (Facebook)","Bespedia.com 2 - FB - PB","HealthySquad.net (Facebook)","MSN Travel (Facebook)","BlackFridayFair.com (Facebook)","HealthPixie.net (Two-Facebook)","Health.online 2 - FB - PB","CrawlerWatch 3 - FB - MS","Zillopedia - FB","Dealsmate 3 - FB","Quelly.net (Facebook)","InsightsAuto.com (Facebook)","Elderlytimes.com(Facebook)","GenieSearch.net (Facebook)","GadgetArchives.com (Facebook)","HealthInsider.net (Facebook-3)","Life-Bliss.com - FB - GM","Dailybundlehub.com - FB - HM","SpeakingHealth - FB - PB","Trenderset.com - FB - HM","HealthDiaries.net (Facebook-2)","Thehealthcluster - 2 - Facebook","Health.online 3 - FB - MT","Mensfocus - FB - PB","Herstyle  - FB - MT","Speakinghealth 2 - FB - MT","InnerSpark - FB - HM","Health.online 4 - FB - MT","Health.online 5 - FB - PB","SpeakingHealth 3 - FB - GM","Innerspark 2 - FB - HM","Herstyle 2 - FB - HM","Speaking Health","BestHealthUpdates - FB","Innerspark 4 - FB - MS","Gotquestion.com (Facebook)","DailyclusterNew3 - FB","EliteSavings.net (Facebook)","TheLifeDigest (FB)","Dailycluster - 2 - Facebook","Mensfocus 2 - FB - GM","Cybermondayusa.com(Facebook)","Local.com Facebook","MSN Websearch (Facebook)","Speakinghealth 4 - FB - PB","MSN Entertainment (Facebook)","blackfridaydealsforyou - FB - 3","Xmas.discount(Facebook)","Dealrings FB - 3","Academida.com 2 - FB - NM","HealthDiaries.net (Facebook)","Innerspark 3 - FB - ED","AutoArena.com (Facebook)","Healthinsider.net Facebook","Investopoly-2 - Facebook","WellnessGuru.net (Facebook-2)","InfoShoppe.net (Facebook)","Classificados - FB","Healthoholic.online - FB - GM","Blackfridaysavings.com(Facebook)","TheHealthDiary.com (FB)","BlackFridayGrabs.com (Facebook)","Thehealthcluster - FB","WeThinkHealth.online - FB - MT","Hubwish.com 2 - FB - PB","MensFocus 3 - FB - MT","HealthMaester.com (Facebook-5)","GameConsole.Deals (FB)","WellCanvas - FB - HM","Thelushhouse - FB","Inquirily.com - FB - MT","DailyclusterNew2 - FB","Dealsmate - Facebook","Academida.com 4 - FB - ED","Updateworld.net - FB - NM","HealthCareToday.net (Facebook-1)","Healthhandbook.online 2 - FB - PB","WellnessGuru.net (Facebook)","TravellersGuide.net(Facebook)","GizmoExpert.net (Facebook)","Healthtonic.net - FB","FAQs.com (Facebook-4)","Allanswers.com-Govt+RE (Facebook)","GenieSearch.net (2-Facebook)","BlackFridaySavings.com (Facebook)","CyberMondayPrices.com (Facebook)","HealthMaester.com (Facebook-2)","Naturalremedees.com - FB - MS","HealthNeeds.net (Facebook)","Bespedia.com - FB - PB","Nurishingly.com - FB - MT","Healthnacity.com 2 - FB - PS","Blackfridaybargains.com (Facebook)","Trenderset.com 2 - FB - PS","Yahoo.com CPG Facebook","Yahoo.com Retail Facebook","TrendingFAQS.com THREE (Facebook)","Yahoo.com DR 1 Facebook","Namestae.com - FB - GM","FAQs.com (Facebook-9)","DealsBlackFriday.net (Facebook)","Healthylivingly.com - FB - HM","CyberMondayStore.net (Facebook)"));
        params.put("accounts",Arrays.asList("10offers.net","2016discounts.com","2017discounts.com","2017prices.com","2018discounts.com","2018prices.com","2019discounts.com","2019offers.net","2019prices.com","2019saver.net","247blackfridaydeals.com","247tempo.com","247wallst.com","academida.com","advicepool.net","aerlie.net","agilefeed.net","all.discount","allanswers.com","allblackfriday.net","alldiscounts.co.uk","alleserdenkliche.com","alllocal.net","allyear.deals","allyearoffers.com","amazedeals.com","answerly.net","answeropedia.net","answersdepot.net","antivirusreviews.com","autoarena.com","autobytel.com","autoculture.online","autoswire.com","averles.com","backpacktales.com","bargain.net","bargainarena.net","bargainopedia.com","bargains.blackfriday","bargainus.net","bespedia.com","besthealthupdates.com","bestofwell.com","beyondthewire.online","bhg.com","blackfridayarena.net","blackfridayay.com","blackfridaybargains.com","blackfridayclub.net","blackfridaydeal4u.com","blackfridaydealsforyou.com","blackfridaydealsuk.net","blackfridaydepot.com","blackfridayelectronics.net","blackfridayfair.com","blackfridayfest.net","blackfridaygrabs.com","blackfridayguru.net","blackfridayhub.net","blackfridaymart.com","blackfridayoferta.com","blackfridaypicks.net","blackfridayprecios.com","blackfridayprices.net","blackfridaysavings.com","blackfridayshoppe.net","blackfridaystar.net","blackfridaysteals.net","blackfridaystore.us","blackfridaystreet.com","blackfridaythunder.com","blackfridayuksale.com","blackfridayus.net","blackfriyay.com","bruckie.com","buscarhoy.net","businessbytes.net","businessfocus.online","businesslink.online","buycentral.us","buynation.net","buystores.net","caloriebee.com","caranddriver.com","cardsexpert.net","cardswallet.com","cartrends.online","cellphones.guru","cheapbargain.net","cheapestdeals.net","cheapmunks.com","choixatoutva.com","christmas.net","christmasoffers.us","clasificados.com","collegearena.com","compare.online","comparecards.online","comparemarts.com","compareopedia.com","comparepoint.us","connexity.com","crawlerwatch.com","cybermonday247.com","cybermonday365.com","cybermondayarena.com","cybermondayay.com","cybermondaycart.com","cybermondayclub.net","cybermondaydaily.com","cybermondayfest.com","cybermondaymania.net","cybermondaynow.com","cybermondayoferta.com","cybermondayoffers.com","cybermondaypicks.com","cybermondayprices.com","cybermondaypro.com","cybermondayrush.com","cybermondaysales.net","cybermondaystore.net","cybermondaystores.net","cybermondaystreet.com","cybermondayuk.net","cybermondayus.com","cybermondayusa.com","cybermondayzone.com","dailybuffer.online","dailybundlehub.com","dailycluster.com","dailyshoppingtrends.com","dailytrend.online","dealatopia.com","dealfinders.net","dealrings.com","deals123.co.uk","deals123.net","deals2017.net","deals2018.com","deals2019.com","dealsandsteals.com","dealsblackfriday.net","dealscart.net","dealscybermonday.net","dealsdivine.com","dealsfarms.com","dealspanda.net","dealstores.net","dealstrooper.com","dealszone.co.uk","dealyscoop.com","degreescompared.com","degreesleuth.com","descubrehoy.net","directory.site","discountarena.net","discover.guru","discoverlocal.net","divatrends.net","divinecaroline.com","driverculture.online","eatingwell.com","elbuscador.net","elderlytimes.com","electronicsinsider.net","electronicsoutlet.online","elitesavings.net","emedicinehealth.com","empirewell.com","empowher.com","entertainmentwebsearch.msn.com","esblackfriday.com","escybermonday.com","ethanstores.com","expertmonks.com","explorelocal.net","familycircle.com","faqs.com","faqslibrary.com","feelingwell.online","financeinja.net","financeninja.net","findadeal.us","findanswers.online","finderbee.net","findercrew.net","findermonk.com","firstup.co","fitnessmagazine.com","focusedbiz.com","forbes.com","frblackfriday.com","gadgetarchives.com","gadgetoffers.com","gadgetsonsale.net","gameconsole.deals","genieadvice.com","geniesage.com","geniesearch.net","geniusearch.co.uk","getbestbargains.com","getmoredaily.com","gizmoexpert.net","gizmozarena.com","glowwise.com","gotquestion.com","grabdiscounts.net","happypaws.online","healdove.com","health.online","healthaccess.com","healthatoz.net","healthcare.com","healthcarepanda.com","healthcaretoday.net","healthdiaries.net","healthfocus.online","healthhandbook.online","healthinsider.net","healthmaester.com","healthnacity.com","healthneeds.net","healthoholic.online","healthorigins.com","healthpixie.net","healthproadvice.com","healthtonic.net","healthyfi.net","healthylivingly.com","healthysquad.net","herstyle.online","hertempo.com","homeandmore.us","homegardenhub.net","hubpages.com","hubwish.com","icananswerthat.com","immediatediscounts.com","infodigest.co.uk","infoinsider.net","infopundit.net","information.com","infosage.net","infosavant.net","infoshoppe.net","innerspark.online","inquirily.com","insightsauto.com","insightspanda.com","instahealthdaily.com","investingbytes.com","jeevesknows.com","jennystores.com","kipen.net","kitchensdeals.com","knowhubb.com","knownemo.com","laptop.blackfriday","laptopsinsider.com","life-bliss.com","listscoop.com","livebtter.com","livedailyhealth.com","liveretailtherapy.com","local.com","localbytes.net","localinsights.net","localwizard.net","loveofretail.net","medicinenet.com","mensfocus.online","midwestliving.com","millesdecouvertes.com","mindbodygreen.com","mobiles.blackfriday","mobiles247.net","mobilesarena.com","mobilesinsider.com","mobileswizard.com","more.com","mortow.com","mranswerable.com","mrlocal.com","mrwiki.com","msn.com","muchascosas.net","mundo.com","myblackfriday.net","namestae.com","naturalremedees.com","naturefrsh.com","nobbyn.com","nowsales.net","nurishedhealth.com","nurishingly.com","ofertascybermondayus.com","offers247.net","offersgarden.com","offerslounge.com","offerspod.com","offersportal.net","offersteals.com","onestoparena.com","onestopstore.net","onlineshopvergleich.com","optimumauto.com","ouidiscount.com","parenting.com","parents.com","patientslounge.com","patteo.net","pazton.com","pearlywhytes.com","priceocity.com","primalhealth.online","qualdo.net","qualityhealth.com","quelly.net","questguru.net","qwedsa.com","rachaelraymag.com","raiseyourstakes.co.uk","realhealth.online","recipe.com","resultsarena.com","resultsbee.com","resultsdigest.com","resultsfeed.net","resultsgator.com","resultsinsights.com","resultspanda.com","retailholics.com","rxlist.com","sales365.net","saverdeals.net","saverprices.net","saversarena.com","savingpanda.co.uk","savingpanda.com","savings2018.com","savings2019.com","savingshub.net","savvymonk.com","searchanswers.net","searchinsider.net","searchinstantly.net","seasondeals.store","seeknemo.com","selectdeals.net","semtest.com","seniorsinsider.net","shopdealio.co.uk","shopdealio.net","shoponym.co.uk","shoponym.com","shopperden.com","shoppersarena.net","shoppersfest.com","shoppersphere.com","shopperspoint.net","shoppersscope.com","shoppingtherapy.online","smartdiscounts.net","speakinghealth.com","speaklocal.net","suchdichgluecklich.com","superrecherche.com","supreme.deals","swiftfeed.net","swipewiki.com","taboola.com","targeteddeals.com","techdealsforyou.net","test.com","thebargains.store","thebizcluster.com","thedealarena.com","thedealgarden.com","thedealsmate.com","thedealstream.com","thediscount.store","thediscountfarm.com","thediscountmill.com","thehealthcluster.com","thehealthdiary.com","thehealthyworld.net","theinfodigest.com","theinvestopoly.com","thelifedigest.com","thelushhouse.com","theprettylife.online","theseniorguardian.com","thestreet.com","thethriftydeals.com","thinkoffers.net","thunderdeals.net","tipsforhealth.com","top10antivirusratings.com","top10ratings.com","topichomes.com","topvarsity.com","toughnickel.com","travellersguide.net","trenderset.com","trendingfaqs.com","trendsanddeals.com","tvsales.blackfriday","tvsinsider.com","uberscene.com","ukblackfridaydeals.com","ukcybermonday.com","ukdealsteals.com","ultimate.careers","ultimatebargains.net","updateworld.net","uphealthnewz.com","usablackfriday.com","usacybermonday.com","usdealsteals.com","usedcarinsights.com","usoffers.net","usphones.net","uspricedrops.com","vitalhealth.online","wantbargain.com","webmd.com","webtrends.co","wellagic.com","wellcanvas.com","welldigest.com","wellnessdaily.net","wellnessguru.net","wellseries.com","wethinkhealth.online","xmas.discount","xmasdiscounts.net","yahoo.com","yearend.discount","yearendsale.co.uk","youmemindbody.com","zillopedia.com","ziphip.com","zoobyte.com","zoomcorner.com"));
        try {
            params.put(Constants.DRUID_CLUSTERS, clusterAccessor.getDruidClusterList());
        } catch (IOException e) {
            log.error("Failed to retrieve list of existing Druid clusters!", e);
            params.put(Constants.DRUID_CLUSTERS, new ArrayList<>());
        }
        return new ModelAndView(params, "accountJobForm");
    }

    public static ModelAndView processAccountInstantAnomalyJob(Request request, Response response) {
        log.info("Getting user query from request.");
        Map<String, Object> params = new HashMap<>(defaultParams);
        Map<String, Object> tableParams = new HashMap<>(defaultParams);
        params.put(Constants.TITLE, "Anomaly Report");
        try {
            Map<String, String> paramsMap = Utils.queryParamsToStringMap(request.queryMap());
            UserQuery userQuery = UserQuery.fromQueryParams(request.queryMap());
            // regenerate user query
            Granularity granularity = Granularity.getValue(paramsMap.get("granularity"));
            Integer granularityRange = userQuery.getGranularityRange();
            Integer hoursOfLag = clusterAccessor.getDruidCluster(paramsMap.get("clusterId")).getHoursOfLag();
            Integer intervalEndTime;
            ZonedDateTime endTime = TimeUtils.parseDateTime(userQuery.getQueryEndTimeText());
            if (ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag).toEpochSecond() < endTime.toEpochSecond()) {
                intervalEndTime = granularity.getEndTimeForInterval(endTime.minusHours(hoursOfLag));
            } else {
                intervalEndTime = granularity.getEndTimeForInterval(endTime);
            }
            Query query = serviceFactory.newDruidQueryServiceInstance().build(userQuery.getQuery(), granularity, granularityRange, intervalEndTime, userQuery.getTimeseriesRange());
            JobMetadata job = new JobMetadata(userQuery, query);
            job.setFrequency(granularity.toString());
            job.setEffectiveQueryTime(intervalEndTime);
            // set egads config
            EgadsConfig config;
            config = EgadsConfig.fromProperties(EgadsConfig.fromFile());
            config.setTsModel(userQuery.getTsModels());
            config.setAdModel(userQuery.getAdModels());
            // detect anomalies
            List<EgadsResult> egadsResult = serviceFactory.newDetectorServiceInstance().detectWithResults(
                    query,
                    job.getSigmaThreshold(),
                    clusterAccessor.getDruidCluster(job.getClusterId()),
                    userQuery.getDetectionWindow(),
                    config
            );
            // results
            List<Anomaly> anomalies = new ArrayList<>();
            List<ImmutablePair<Integer, String>> timeseriesNames = new ArrayList<>();
            int i = 0;
            for (EgadsResult result : egadsResult) {
                anomalies.addAll(result.getAnomalies());
                timeseriesNames.add(new ImmutablePair<>(i++, result.getBaseName()));
            }
            List<AnomalyReport> reports = serviceFactory.newJobExecutionService().getReports(anomalies, job);
            tableParams.put(Constants.INSTANTVIEW, "true");
            tableParams.put(DatabaseConstants.ANOMALIES, reports);
            params.put("tableHtml", thymeleaf.render(new ModelAndView(tableParams, "table")));
            Type jsonType = new TypeToken<EgadsResult.Series[]>() { }.getType();
            params.put("data", new Gson().toJson(EgadsResult.fuseResults(egadsResult), jsonType));
            params.put("timeseriesNames", timeseriesNames);
        } catch (IOException | ClusterNotFoundException | DruidException | SherlockException e) {
            log.error("Error while processing instant job!", e);
            params.put(Constants.ERROR, e.toString());
        } catch (Exception e) {
            log.error("Unexpected error!", e);
            params.put(Constants.ERROR, e.toString());
        }
        return new ModelAndView(params, "reportInstant");
    }
}
