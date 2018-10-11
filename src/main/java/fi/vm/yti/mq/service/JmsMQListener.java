package fi.vm.yti.mq.service;

import fi.vm.yti.security.AuthenticatedUserProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Session;

@Component
public class JmsMQListener {
    private final String subSystem;
    private final String incomingQueue;

    // JMS-client
    @Autowired
    private JmsMessagingTemplate jmsMessagingTemplate;

    @Autowired
    public JmsMQListener(JmsMessagingTemplate jmsMessagingTemplate,
                      @Value("${mq.active.subsystem}") String subSystem) {
        this.jmsMessagingTemplate = jmsMessagingTemplate;
        this.subSystem = subSystem;
        this.incomingQueue = subSystem+"Incoming";
    }

    /**
     * State-handler queue, just receive and move it into the actual processing queue
     *
     * @param message
     * @return
     * @throws JMSException
     */
    @JmsListener(destination =  "${mq.active.subsystem}Incoming")
    @SendTo("${mq.active.subsystem}Processing")
    public Message receiveMessage(final Message message,
                                  Session session,
                                  @Header String jobtoken,
                                  @Header String userId,
                                  @Header String uri) throws JMSException {
        System.out.println("Received and transferred to processing. Message headers=" + message.getHeaders());
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        accessor.copyHeaders(message.getHeaders());
        accessor.setLeaveMutable(true);

        // Send status message
        Message mess = MessageBuilder
                .withPayload("Processing " + uri)
                .setHeaders(accessor)
/*                // Authenticated user
                .setHeader("userId", userId)
                // Token which is used when querying status
                .setHeader("jobtoken", jobtoken)
                .setHeader("subsystem", subSystem)
                .setHeader("uri", uri)
                */
                .build();
        jmsMessagingTemplate.send(subSystem+"Status", mess);
        return message;
    }

}