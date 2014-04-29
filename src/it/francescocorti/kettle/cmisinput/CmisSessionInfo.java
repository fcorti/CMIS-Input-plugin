package it.francescocorti.kettle.cmisinput;

import java.util.Date;

import org.apache.chemistry.opencmis.client.api.Session;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisSessionInfo {

	private Session session;
	private Date date;

	public CmisSessionInfo(Session session, Date date) {
		super();
		this.session = session;
		this.date = date;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public Date getDate() {
		return date;
	}

	public long getTime() {
		return getDate().getTime();
	}

	public void setDate(Date date) {
		this.date = date;
	}
}
