package fi.vm.yti.mq.service;

import fi.vm.yti.security.AuthenticatedUserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.core.BrowserCallback;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import java.util.*;

@Service
@EnableJms
public class JMSService {

    private final AuthenticatedUserProvider userProvider;
    private final String namespaceRoot;
    private final String subSystem;

    private static final Logger logger = LoggerFactory.getLogger(JMSService.class);

    public static final String[] SET_VALUES = new String[] { "Vocabulary", "Test", "CodeList" };
    public static final Set<String> SUPPORTED_SYSTEMS = new HashSet<>(Arrays.asList(SET_VALUES));

    // JMS-client
    @Autowired
    private JmsMessagingTemplate jmsMessagingTemplate;

    @Autowired
    public JMSService(AuthenticatedUserProvider userProvider,
                      JmsMessagingTemplate jmsMessagingTemplate,
                      @Value("${namespace.root}") String namespaceRoot,
                      @Value("${mq.active.subsystem}") String subSystem) {
        this.userProvider = userProvider;
        this.jmsMessagingTemplate = jmsMessagingTemplate;
        this.namespaceRoot = namespaceRoot;
        this.subSystem = subSystem;
    }

    public int getStatus(UUID jobtoken){
        // Status not_found/running/errors
        System.out.println(subSystem+" ReadyState="+getJobState(jobtoken, subSystem+"Ready"));
        // Status not_found/running/errors
        // Query status information from ActiveMQ
        Boolean stat = getJobState(jobtoken, subSystem+"Status");
        System.out.println("Check Status:"+stat.booleanValue());
        stat = getJobState(jobtoken, subSystem+"Ready");
        System.out.println("Check ready:"+stat.booleanValue());

        if (getJobState(jobtoken, subSystem+"Incoming").booleanValue()){
            logger.warn("Import operation already started for "+jobtoken);
            return  HttpStatus.NOT_ACCEPTABLE.value();
        } else if (getJobState(jobtoken, subSystem+"Status").booleanValue()){
            if(logger.isDebugEnabled())
                logger.debug("Processing"+jobtoken);
            return HttpStatus.PROCESSING.value();
        } else if (getJobState(jobtoken, subSystem+"Processing").booleanValue()){
            logger.debug("Processing"+jobtoken);
            return HttpStatus.PROCESSING.value();
        } else if(getJobState(jobtoken, subSystem+"Ready").booleanValue()){
            System.out.println("Ready found:");
            if(logger.isDebugEnabled())
                logger.debug("Import done for "+jobtoken);
            return HttpStatus.OK.value();
        }
        return  HttpStatus.NO_CONTENT.value();
    }

    private Boolean getJobState(UUID jobtoken,String queueName) {
        return jmsMessagingTemplate.getJmsTemplate().browseSelected(queueName, "jobtoken='"+jobtoken.toString()+"'",new BrowserCallback<Boolean>() {
            @Override
            public Boolean doInJms(Session session, QueueBrowser browser) throws JMSException {
                Enumeration messages = browser.getEnumeration();
//                return  messages.hasMoreElements();
                return  new Boolean(messages.hasMoreElements());
            }
        });
    }

    public boolean checkIfImportIsRunnig(String uri) {
        Boolean rv = false;
        System.out.println("Check status for "+subSystem);
        if (checkUriStatus(uri, subSystem+"Status").booleanValue()) {
            rv = true;
        } else if (checkUriStatus(uri, subSystem+"Processing").booleanValue()) {
            rv = true;
        }
        return rv;
    }

    public Boolean checkUriStatus(String uri, String queueName) {
        return jmsMessagingTemplate.getJmsTemplate().browseSelected(queueName, "uri='"+uri+"'",new BrowserCallback<Boolean>() {
            @Override
            public Boolean doInJms(Session session, QueueBrowser browser) throws JMSException {
                Enumeration messages = browser.getEnumeration();
                return  new Boolean(messages.hasMoreElements());
            }
        });
    }

    public int handleImportAsync(MessageHeaderAccessor headers, String subsystem, String uri, String payload) {

        System.out.println("handleImportAsync subsystem:"+subsystem+" Uri:"+uri);
        if (!SUPPORTED_SYSTEMS.contains(subsystem)) {
            logger.error("Unsupported subsystem:<" + subsystem + "> (Currently supported subsystems: "+SET_VALUES);
            return  HttpStatus.NOT_ACCEPTABLE.value();
        }

        // Check uri
        if(checkIfImportIsRunnig(uri)){
            logger.error("Import running for URI:<" + uri + ">");
            return  HttpStatus.CONFLICT.value();
        }

        UUID operationId=UUID.randomUUID();
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        if (headers != null) {
            // Add application specific headers
            accessor.copyHeaders(headers.toMap());
        }
        accessor.setHeader("userId",  userProvider.getUser().getId().toString());
        accessor.setHeader("jobtoken", operationId.toString());
        accessor.setHeader("system", subsystem);
        accessor.setHeader("uri", uri);
            Message mess = MessageBuilder
                    .withPayload(payload)
                    .setHeaders(accessor)
 /*                   // Authenticated user
                    .setHeader("userId", userProvider.getUser().getId().toString())
                    // Token which is used when querying status
                    .setHeader("jobtoken", operationId.toString())
                    .setHeader("system", subsystem)
                    // Target queue
                    .setHeader("uri", uri)
                    */
                    .build();
            jmsMessagingTemplate.send(subsystem+"Incoming", mess);
        return  HttpStatus.OK.value();
    }

    public Message processMessage(final Message message,Session session,
                                  @Header String jobtoken,
                                  @Header String userId,
                                  @Header String format,
                                  @Header String vocabularyId,
                                  @Header String uri) throws JMSException {
        // Consume incoming
        System.out.println("Process message "+ message.getHeaders());
        System.out.println("session= "+ session);
        System.out.println("UserId="+userId);
        String payload ="{}";
        if(jmsMessagingTemplate == null){
            System.out.println("MessagingTemplate not initialized!!!!!");
        }

        // Set import as handled. IE. consume processed message
        setReady(jobtoken);
        // Set result as a payload and move it to ready-queue
        Message mess = MessageBuilder
                .withPayload(payload)
                // Authenticated user
                .setHeader("userId", userId)
                // Token which is used when querying status
                .setHeader("jobtoken", jobtoken)
                .setHeader("format",format)
                // Target vocabulary
                .setHeader("vocabularyId", vocabularyId)
                .setHeader("uri", uri)
                .build();
        return mess;
    }

    public void setReady(String jobtoken) {
        System.out.println("Consume Processed item from:"+subSystem+"Status");
        try {
            javax.jms.Message m = jmsMessagingTemplate.getJmsTemplate().receiveSelected(subSystem+"Status", "jobtoken='" + jobtoken.toString() + "'");
            System.out.println("Deleting "+m.getStringProperty("jobtoken"));
        } catch (JMSException jex) {
            jex.printStackTrace();
        }
    }
}

