package it.francescocorti.kettle.cmisinput;

import java.util.List;
import java.util.Map;

import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyBooleanImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyDateTimeImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyDecimalImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyIntegerImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyStringImpl;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisInputMeta extends BaseStepMeta implements StepMetaInterface {

    private String url;
    private String login;
    private String password;
    private String cmisQuery;
    private CmisSessionFactory sessions;

	public CmisInputMeta() {
        super();
    }

    public Object clone() {
    	CmisInputMeta retval = (CmisInputMeta)super.clone();
        return retval;
    }

    public void setDefault() {
        url = "http://localhost:8080/alfresco/service/api/cmis";
        login = "admin";
        password = "admin";
        cmisQuery = "select * from cmis:document";
    }

    public String getXML() {
        StringBuffer retval = new StringBuffer();
        retval.append("    " + XMLHandler.addTagValue("url",url));
        retval.append("    " + XMLHandler.addTagValue("login",login));
        retval.append("    " + XMLHandler.addTagValue("password",password));
        retval.append("    " + XMLHandler.addTagValue("sql",cmisQuery));
        return retval.toString();
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException {
        readData(stepnode, databases);
    }
    
    private void readData(Node stepnode, List<? extends SharedObjectInterface> databases) throws KettleXMLException {
        try {
            url = XMLHandler.getTagValue(stepnode, "url");
            login = XMLHandler.getTagValue(stepnode, "login");
            password = XMLHandler.getTagValue(stepnode, "password");
            cmisQuery = XMLHandler.getTagValue(stepnode, "sql");
        }
        catch(Exception e) {
            throw new KettleXMLException("Unable to load step info from XML", e);
        }
    }

    public void getFields(RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException {
        try {
            // Execute query.
    		logDebug("Cmis Input - Retrieving the session.");
        	Session session = getSession(space);
    		logDebug("Cmis Input - Executing the query.");
    	    ItemIterable<QueryResult> results = session.query(space.environmentSubstitute(getCmisQuery()), false);
    	    logRowlevel("Cmis Input - Retrieved " + results.getPageNumItems() + " results.");
    	    if (results.getPageNumItems() == 0) return;
    	    QueryResult result = results.iterator().next();

    	    // Extract metadata.
    		logDebug("Cmis Input - Defining the metadata.");
    	    RowMetaInterface rowMeta = new RowMeta();
            for (PropertyData<?> property : result.getProperties()) {

                ValueMeta v = new ValueMeta();

		    	logRowlevel("Cmis Input - Property: '" + property.getQueryName() + "'='" + property.getFirstValue() + "'");
                if (property.getClass().isInstance(new PropertyStringImpl()))
                	v.setType(ValueMeta.TYPE_STRING);
                else if (property.getClass().isInstance(new PropertyBooleanImpl()))
                	v.setType(ValueMeta.TYPE_BOOLEAN);
                else if (property.getClass().isInstance(new PropertyDateTimeImpl()))
                	v.setType(ValueMeta.TYPE_DATE);
                else if (property.getClass().isInstance(new PropertyDecimalImpl()))
                	v.setType(ValueMeta.TYPE_NUMBER);
                else if (property.getClass().isInstance(new PropertyIntegerImpl()))
                	v.setType(ValueMeta.TYPE_INTEGER);
                else
                	v.setType(ValueMeta.TYPE_STRING);

                v.setName(property.getQueryName());
                v.setOrigin(origin);
                rowMeta.addValueMeta(v);
            }
            row.addRowMeta(rowMeta);

        }
        catch (Exception e) {
            throw new KettleStepException("Unable to get queryfields for SQL: " + Const.CR + space.environmentSubstitute(getCmisQuery()), e);
        }
    }

    public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
        try {
            url = rep.getStepAttributeString(id_step, "url");
            login = rep.getStepAttributeString(id_step, "login");
            password = rep.getStepAttributeString(id_step, "password");
            cmisQuery = rep.getStepAttributeString(id_step, "sql");
        }
        catch(Exception e) {
            throw new KettleException("Unexpected error reading step information from the repository", e);
        }
    }
    
    public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "url", url);
            rep.saveStepAttribute(id_transformation, id_step, "login", login);
            rep.saveStepAttribute(id_transformation, id_step, "password", password);
            rep.saveStepAttribute(id_transformation, id_step, "sql", cmisQuery);
        }
        catch(Exception e) {
            throw new KettleException("Unable to save step information to the repository for id_step="+id_step, e);
        }
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) {

    	@SuppressWarnings("unused")
		CheckResult cr;

        /* TODO
        if (databaseMeta!=null) {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection exists", stepMeta);
            remarks.add(cr);

            Database db = new Database(loggingObject, databaseMeta);
            db.shareVariablesWith(transMeta);
            databases = new Database[] { db }; // keep track of it for canceling purposes...

            try {
                db.connect();
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Connection to database OK", stepMeta);
                remarks.add(cr);

                if (sql!=null && sql.length()!=0) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "SQL statement is entered", stepMeta);
                    remarks.add(cr);
                }
                else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "SQL statement is missing.", stepMeta);
                    remarks.add(cr);
                }
            }
            catch(KettleException e) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "An error occurred: "+e.getMessage(), stepMeta);
                remarks.add(cr);
            }
            finally {
                db.disconnect();
            }
        }
        else {
            cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Please select or create a connection to use", stepMeta);
            remarks.add(cr);
        }
        
        // See if we have an informative step...
        StreamInterface infoStream = getStepIOMeta().getInfoStreams().get(0);
        if (!Const.isEmpty(infoStream.getStepname())) {
            boolean found=false;
            for (int i=0;i<input.length;i++) {
                if (infoStream.getStepname().equalsIgnoreCase(input[i])) found=true;
            }
            if (found) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "Previous step to read info from ["+infoStream.getStepname()+"] is found.", stepMeta);
                remarks.add(cr);
            }
            else {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Previous step to read info from ["+infoStream.getStepname()+"] is not found.", stepMeta);
                remarks.add(cr);
            }
            
            // Count the number of ? in the SQL string:
            int count=0;
            for (int i=0;i<sql.length();i++) {
                char c = sql.charAt(i);
                if (c=='\'') // skip to next quote!
                {
                    do {
                        i++;
                        c = sql.charAt(i);
                    }
                    while (c!='\'');
                }
                if (c=='?') count++;
            }
            // Verify with the number of informative fields...
            if (info!=null) {
                if(count == info.size()) {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "This step is expecting and receiving "+info.size()+" fields of input from the previous step.", stepMeta);
                    remarks.add(cr);
                }
                else {
                    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "This step is receiving "+info.size()+" but not the expected "+count+" fields of input from the previous step.", stepMeta);
                    remarks.add(cr);
                }
            }
            else {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Input step name is not recognized!", stepMeta);
                remarks.add(cr);
            }
        }
        else {
            if (input.length>0) {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, "Step is not expecting info from input steps.", stepMeta);
                remarks.add(cr);
            }
            else {
                cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, "No input expected, no input provided.", stepMeta);
                remarks.add(cr);
            }
        }
            */
    }

    /**
     * @param steps optionally search the info step in a list of steps
    public void searchInfoAndTargetSteps(List<StepMeta> steps) {
      for (StreamInterface stream : getStepIOMeta().getInfoStreams()) {
        stream.setStepMeta( StepMeta.findStep(steps, (String)stream.getSubject()) );
      }
    }
     */

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans) {
        return new CmisInput(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new CmisInputData();
    }

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getCmisQuery() {
		return cmisQuery;
	}

	public void setCmisQuery(String cmisQuery) {
		this.cmisQuery = cmisQuery;
	}

	public CmisSessionFactory getSessions() {
		return sessions;
	}

	public Session getSession(VariableSpace space) {

		String logMessage = "";
		logMessage += "Cmis Input - Getting session (";
		if (CmisSessionFactory.exists(
				space.environmentSubstitute(getUrl()),
				space.environmentSubstitute(getLogin()),
				space.environmentSubstitute(getPassword())))
				logMessage += "still exist";
			else
				logMessage += "created";
		logMessage += " and ";
		if (CmisSessionFactory.isExpired(
				space.environmentSubstitute(getUrl()),
				space.environmentSubstitute(getLogin()),
				space.environmentSubstitute(getPassword())))
				logMessage += "expired";
			else
				logMessage += "not expired";
		logMessage += ").";
		logDebug(logMessage);

		return CmisSessionFactory.getSession(
			space.environmentSubstitute(getUrl()),
			space.environmentSubstitute(getLogin()),
			space.environmentSubstitute(getPassword()));
	}

    /*
    public void analyseImpact(List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info) throws KettleStepException {
        // Find the lookupfields...
        RowMetaInterface out = new RowMeta(); 
        // TODO: this builds, but does it work in all cases.
        getFields(out, stepMeta.getName(), new RowMetaInterface[] { info }, null, transMeta);
        
        if (out!=null) {
            for (int i=0;i<out.size();i++) {
                ValueMetaInterface outvalue = out.getValueMeta(i);
                DatabaseImpact ii = new DatabaseImpact( DatabaseImpact.TYPE_IMPACT_READ, 
                                                transMeta.getName(),
                                                stepMeta.getName(),
                                                databaseMeta.getDatabaseName(),
                                                "",
                                                outvalue.getName(),
                                                outvalue.getName(),
                                                stepMeta.getName(),
                                                sql,
                                                "read from one or more database tables via SQL statement"
                                                );
                impact.add(ii);

            }
        }
    }
    */
    /*
    public DatabaseMeta[] getUsedDatabaseConnections() {
        if (databaseMeta!=null) {
            return new DatabaseMeta[] { databaseMeta };
        }
        else {
            return super.getUsedDatabaseConnections();
        }
    }
    */
}
