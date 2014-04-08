package it.francescocorti.kettle.cmisinput;

import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyDateTimeImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyDecimalImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyIntegerImpl;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisInput extends BaseStep implements StepInterface {

    private CmisInputData data;
	private CmisInputMeta meta;

	public CmisInput(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s,stepDataInterface,c,t,dis);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		if (!first) {
			setOutputDone();
			return false;
        }
        first = false;

        logDebug("Cmis Input - Starting...");
	    meta = (CmisInputMeta)smi;
	    data = (CmisInputData)sdi;

	    // Define outputRowData.
        logDebug("Cmis Input - Define 'outputRowData'.");
	    data.outputRowMeta = new RowMeta();
	    meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

        // Executing the query.
		logDebug("Cmis Input - Executing the query.");
	    ItemIterable<QueryResult> results = meta.getSession(getParentVariableSpace()).query(getParentVariableSpace().environmentSubstitute(meta.getCmisQuery()), false);

		logBasic("Cmis Input - Retrieving the n." + results.getTotalNumItems() + " query results.");
	    int i = 0;
	    for(QueryResult result : results) {
	    	logRowlevel("Cmis Input - Result n." + i + ".");
		    Object[] outputRow = RowDataUtil.allocateRowData(result.getProperties().size());
		    for(int j=0; j < result.getProperties().size(); ++j) {
		    	PropertyData<?> property = result.getProperties().get(j);

		    	logRowlevel("Cmis Input - Property: '" + property.getQueryName() + "'='" + property.getFirstValue() + "'");
                if (property.getClass().isInstance(new PropertyDateTimeImpl()))
    	        	outputRow[j] = new Date(((GregorianCalendar) property.getFirstValue()).getTimeInMillis());
                else if (property.getClass().isInstance(new PropertyIntegerImpl()))
    	        	outputRow[j] = ((BigInteger)property.getFirstValue()).longValue();
                else if (property.getClass().isInstance(new PropertyDecimalImpl()))
    	        	outputRow[j] = ((BigInteger)property.getFirstValue()).doubleValue();
                else
                	outputRow[j] = property.getFirstValue();
		    }

		    logRowlevel("Cmis Input - PutRow n." + i + ".");
			putRow(data.outputRowMeta, outputRow);

			if (checkFeedback(getLinesRead()))
				logBasic("Cmis Input - Linenr " + getLinesRead());  // Some basic logging every 5000 rows.

			++i;
		}

        logDebug("Cmis Input - Ending...");
		return true;
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
	    meta = (CmisInputMeta)smi;
	    data = (CmisInputData)sdi;
	    return super.init(smi, sdi);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
	    meta = (CmisInputMeta)smi;
	    data = (CmisInputData)sdi;
	    super.dispose(smi, sdi);
	}
}
