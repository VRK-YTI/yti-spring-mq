package fi.vm.yti.mq.service;

import fi.vm.yti.security.AuthenticatedUserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Service;

import javax.jms.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.*;

@Service
@EnableJms
public class YtiMQService {

    private final AuthenticatedUserProvider userProvider;

    private final String subSystem;

    private static final Logger logger = LoggerFactory.getLogger(YtiMQService.class);

    public static final String[] SET_VALUES = new String[] { "Terminology", "Test", "CodeList" };
    public static final Set<String> SUPPORTED_SYSTEMS = new HashSet<>(Arrays.asList(SET_VALUES));

    // JMS-client
    private JmsMessagingTemplate jmsMessagingTemplate;

    private JmsMessagingTemplate jmsTopicClient;
    // Define public status values
    public final static int STATUS_PREPROCESSING = 1;
    public final static int STATUS_PROCESSING = 2;
    public final static int STATUS_READY = 3;

    // key(token as UUID or uri), message pauload as String)

    // Add followiing into the using appöication.properties
    // spring.cache.cache-names: Terminology
    // spring.cache.caffeine.spec: maximumSize=100, expireAfterAccess=30s

    // Cache containing key,payload as JMS message
//    Cache<String, Message> statusCache = Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(500).build(); 
    Cache<String, Message> statusCache = Caffeine.newBuilder().build(); 

    @Autowired
    public YtiMQService(AuthenticatedUserProvider userProvider,
                        JmsMessagingTemplate jmsMessagingTemplate,
                        @Value("${mq.active.subsystem}") String subSystem) {
        this.userProvider = userProvider;
        this.jmsMessagingTemplate = jmsMessagingTemplate;
        this.subSystem = subSystem;

        // Initialize topic connection
        jmsTopicClient = new JmsMessagingTemplate(jmsMessagingTemplate.getConnectionFactory());
        jmsTopicClient.getJmsTemplate().setPubSubDomain(true);
    }

    public void removeStatusMessage(String uri) {
        // Use jobid or uri as a key
        logger.info("Removing status cache with "+ uri );
        statusCache.invalidate(uri);
    }

    public void removeStatusMessage(UUID token) {
        // Use jobid or uri as a key
        logger.info("Removing status cache with "+ token );
        statusCache.invalidate(token);
    }


    /**
     * State-handler queue, just receive and update internal cache
     *
     * @param message
     * @return
     * @throws JMSException
     */
    @JmsListener(destination =  "${mq.active.subsystem}Incoming")
    @SendTo("${mq.active.subsystem}Processing")
    public Message receiveMessage(final Message message,
                                  Session session,
                                  @Header("jobtoken") String jobtoken,
                                  @Header String userId,
                                  @Header String uri) throws JMSException {
        if(logger.isDebugEnabled()){
            logger.debug("Received and transferred to processing. Message headers=" + message.getHeaders());
        }
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        accessor.copyHeaders(message.getHeaders());
        accessor.setLeaveMutable(true);

        // Send status message
        Message mess = MessageBuilder
                .withPayload("Processing " + uri)
                .setHeaders(accessor)
                .build();
        jmsMessagingTemplate.send(subSystem+"Status", mess);
        return message;
    }

    public HttpStatus getStatus(UUID jobtoken){

        Message mess = statusCache.getIfPresent(jobtoken.toString());
        if(mess!=null) {
            logger.info("Status for given jobtoken:" + mess.getPayload().toString());
            int status=(int)mess.getHeaders().get("status");
            switch(status){
                case YtiMQService.STATUS_READY:{
                    System.out.println("Ready found:");
                    if(logger.isDebugEnabled())
                        logger.debug("Import done for "+jobtoken);
                    return HttpStatus.OK;
                }
                case YtiMQService.STATUS_PROCESSING:{
                    if(logger.isDebugEnabled())
                        logger.debug("Processing "+jobtoken);
                    System.out.println("Timestamp="+(long)mess.getHeaders().get("timestamp"));
                    long expirationtime=System.currentTimeMillis() - (long)mess.getHeaders().get("timestamp");
                    System.out.println("current_time-stamp="+expirationtime);
                    if( expirationtime > 30 * 1000) {
                        return HttpStatus.OK;
                    } else
                        return HttpStatus.PROCESSING;
                }
                case YtiMQService.STATUS_PREPROCESSING:{
                    logger.warn("Import operation already started for "+jobtoken);
                    return  HttpStatus.NOT_ACCEPTABLE;
                }
            }
        }
        else
            System.out.println("Status not found");
        return  HttpStatus.NO_CONTENT;
    }

    public HttpStatus getStatus(UUID jobtoken, StringBuffer payload){
        // Status not_found/running/errors
        logger.info("Current Status for given jobtoken:"+statusCache.getIfPresent(jobtoken.toString()));
        // Query status information from ActiveMQ
        System.out.println("getStatus with payload:");
        Message mess = statusCache.getIfPresent(jobtoken.toString());
        if(mess!=null) {
            // return also payload
            payload.append(mess.getPayload());
            if(logger.isDebugEnabled()){
                logger.debug("Get Current Status for given jobtoken:" + mess.getPayload().toString());
            }
            int status=(int)mess.getHeaders().get("status");
            switch(status){
                case YtiMQService.STATUS_READY:{
                    System.out.println("Ready found:");
                    if(logger.isDebugEnabled())
                        logger.debug("Import done for "+jobtoken);
                    return HttpStatus.OK;
                }
                case YtiMQService.STATUS_PROCESSING:{
                    if(logger.isDebugEnabled())
                        logger.debug("Processing "+jobtoken);
                        return HttpStatus.PROCESSING;
                }
                case YtiMQService.STATUS_PREPROCESSING:{
                    logger.warn("Import operation already started for "+jobtoken);
                    return  HttpStatus.NOT_ACCEPTABLE;
                }
            }
        }
        if(logger.isDebugEnabled()){
            Map<String,Message> map = statusCache.asMap();
            Set<String> keys=map.keySet();
            keys.forEach(k-> {
                System.out.println("KEY="+k);
            });
        }
        return  HttpStatus.NO_CONTENT;
    }

    public boolean checkIfImportIsRunning(String uri) {
        Boolean rv = false;
        logger.info("checkIfImport running for:"+uri);
//        if(logger.isDebugEnabled()){
            Map<String,Message> map = statusCache.asMap();
            Set<String> keys=map.keySet();
            keys.forEach(k-> {
                System.out.println("KEY="+k);
            });
//        }

        // Check cached status first running if not ready
        Message mess = statusCache.getIfPresent(uri);
        if(mess!=null){
            int status = (int)mess.getHeaders().get("status");
            logger.info("checkIfImportIsRunning found uri with state:"+status+"\n"+mess);
            if(status == YtiMQService.STATUS_PROCESSING || status == YtiMQService.STATUS_PREPROCESSING) {
                System.out.println("Timestamp="+(long)mess.getHeaders().get("timestamp"));
                rv =true;
            }
            // Cache found, use it
            return rv;
        }
        else{
            System.out.println("Not cached item found for "+uri);
        }
        return rv;
    }

    public int handleImportAsync(UUID jobtoken, MessageHeaderAccessor headers, String subsystem, String uri, String payload) {

        System.out.println("handleImportAsync subsystem:"+subsystem+" Uri:"+uri);
        if (!SUPPORTED_SYSTEMS.contains(subsystem)) {
            logger.error("Unsupported subsystem:<" + subsystem + "> (Currently supported subsystems: "+SET_VALUES);
            return  HttpStatus.NOT_ACCEPTABLE.value();
        }

        // Check uri
/*        
        if(checkIfImportIsRunning(uri)){
            logger.error("Import running for URI:<" + uri + ">");
            return  HttpStatus.CONFLICT.value();
        }
*/
        // If jobtoken is not set, create new one
        if(jobtoken == null)
            jobtoken=UUID.randomUUID();
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        if (headers != null) {
            // Add application specific headers
            accessor.copyHeaders(headers.toMap());
        }
        // Authenticated user
        System.out.println("UserProvider="+userProvider+" user="+userProvider.getUser());
        String userId = userProvider.getUser().getId() != null ? userProvider.getUser().getId().toString(): "00000000-0000-0000-0000-000000000000";
        accessor.setHeader("userId",  userId);

        // Token which is used when querying status
        accessor.setHeader("jobtoken", jobtoken.toString());
        // Target identification data
        accessor.setHeader("system", subsystem);
        accessor.setHeader("uri", uri);
        // Use jobtoken as correlationID
        accessor.setHeader("JMSCorrelationID",jobtoken.toString());

        Message mess = MessageBuilder
                    .withPayload(payload)
                    .setHeaders(accessor)
                    .build();
            // send item for processing
        if(logger.isDebugEnabled()){
            logger.debug("Send job:"+jobtoken+" to the processing queue:"+subsystem+"Incoming");
        }
        jmsMessagingTemplate.send(subsystem+"Incoming", mess);
        return  HttpStatus.OK.value();
    }

    /**
     * Update status information state machine
     */
    @JmsListener(destination = "${mq.active.subsystem}Status")
    public void receiveStatusMessage(final Message message,
                                    Session session,
                                    @Header("jobtoken") String jobtoken,
                                    @Header("userId") String userId,
                                    @Header("uri") String uri)
                                    throws JMSException {
        if(logger.isDebugEnabled()){
            logger.debug("Received Status-Message: headers=" + message.getHeaders());
        }
        // Use jobid or uri as a key
        logger.info("Updating status cache with "+jobtoken + " and " + uri +" using status:"+message.getHeaders().get("status"));
        statusCache.put(jobtoken, message);
        statusCache.put(uri, message);
    }

    // when spring.jms.isPubSubDomain=true  defined in profile we use publish/consumer type.
    @SendTo("${mq.active.subsystem}Status")
    public Message setStatus(int status, String jobtoken, String userId, String uri,  String payload) {

        // Add application specific headers
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        // Authenticated user
        accessor.setHeader("userId",  userId);
        // Token which is used when querying status
        accessor.setHeader("jobtoken", jobtoken.toString());
        //  Use  jobtoken as correlation id
        accessor.setHeader("JMSCorrelationID",jobtoken.toString());
        // Target identification data
        accessor.setHeader("system", subSystem);
        accessor.setHeader("uri", uri);
        // Set status as int
        accessor.setHeader("status",status);

        Message mess = MessageBuilder
                .withPayload(payload)
                .setHeaders(accessor)
                .build();
        // send new  item to Status-queue
        if(mess != null) {
            if(logger.isDebugEnabled()){
                logger.debug("Send status message to topic:");
            }
            jmsMessagingTemplate.send(subSystem + "Status", mess);
            if(logger.isDebugEnabled()){
                logger.debug("SEND set STATUS:"+mess);
            }
        }
        return mess;
    }    

    public void viewStatus(){
        Map<String, Message> items = statusCache.asMap();
        if(logger.isDebugEnabled()){
            logger.debug("status keys="+items.keySet());
        }
    }
}
