package it.francescocorti.kettle.cmisinput;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisSessionFactory {

	private final static int sessionDurationInMillisecs = 45 * 60 * 1000;
	private static Map<String, CmisSessionInfo> sessions = new HashMap<>();

	public static Session getSession(String url, String login, String password) {

		String sessionId = getSessionId(url, login, password);

		if (!exists(sessionId)) {
	        getSessions().put(
	        	sessionId, 
	        	new CmisSessionInfo(getNewSession(url, login, password), new Date()));
		}
		else if (isExpired(sessionId)) {
			getSessions().remove(sessionId);
	        getSessions().put(
	        	sessionId, 
	        	new CmisSessionInfo(getNewSession(url, login, password), new Date()));
		}

		return getSessions().get(sessionId).getSession();
	}

	public static Session getNewSession(String url, String login, String password) {

		// Default factory implementation.
	    SessionFactory factory = SessionFactoryImpl.newInstance();
	    Map<String, String> parameter = new HashMap<String, String>();

        // Connection settings.
        parameter.put(SessionParameter.ATOMPUB_URL, url);
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
        parameter.put(SessionParameter.USER, login);
        parameter.put(SessionParameter.PASSWORD, password);

        // Create session.
        List<org.apache.chemistry.opencmis.client.api.Repository> repositories = factory.getRepositories(parameter);
        return repositories.get(0).createSession();
	}

	public static boolean isExpired(String url, String login, String password) {
		return isExpired(getSessionId(url, login, password));
	}

	public static boolean isExpired(String sessionId) {
		return exists(sessionId) && ((new Date()).getTime() > getSessions().get(sessionId).getTime() + sessionDurationInMillisecs);
	}

	public static boolean exists(String url, String login, String password) {
		return exists(getSessionId(url, login, password));
	}

	public static boolean exists(String sessionId) {
		return getSessions().containsKey(sessionId);
	}

	public static String getSessionId(String url, String login, String password) {
		return login + ":" + password + "@" + url;
	}

	public static Map<String, CmisSessionInfo> getSessions() {
		return sessions;
	}
}
