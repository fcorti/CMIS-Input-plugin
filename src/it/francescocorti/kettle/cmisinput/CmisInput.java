package it.francescocorti.kettle.cmisinput;

import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PropertyDateTimeImpl;
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
 * @since 2014-05-01
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

		// Define outputRowMeta.
		logDebug("Cmis Input - Define 'outputRowMeta'.");
		data.outputRowMeta = new RowMeta();
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

		ItemIterable<QueryResult> results = null;
		long skipNum = 0;
		do {

			// Executing the query in pages.
			logDebug("Cmis Input - Executing the query (from: " + skipNum + ").");
			results = meta.getSession(getParentVariableSpace()).query(getParentVariableSpace().environmentSubstitute(meta.getCmisQuery()), false).skipTo(skipNum);
			long pageNumItems = results.getPageNumItems();
			logBasic("Cmis Input - Retrieved n." + results.getPageNumItems() + " results from item n." + skipNum + " on a total of n." + results.getTotalNumItems() + " results.");

			long itemInPage = 0;
			while (itemInPage < pageNumItems) {

				logRowlevel("Cmis Input - Result n." + (skipNum + 1) + " (item in page: " + (itemInPage + 1) + ").");
				QueryResult result = results.iterator().next();

				logRowlevel("Cmis Input - Preparing to put row n." + (skipNum + 1) + " (item in page: " + (itemInPage + 1) + ").");
				int[] outputRowIndex = allocateRowDataIndex(result.getProperties().size());
				int j = 0;
				do {
					logRowlevel("Cmis Input - Put values n." + (++j) + " into the row n." + (skipNum + + 1) + ".");
					Object[] outputRow = buildRowData(result, outputRowIndex);
					putRow(data.outputRowMeta, outputRow);
					outputRowIndex = getNextRowDataIndex(result, outputRowIndex);
				} while (outputRowIndex != null);

				if ((skipNum + 1) % 5000 == 0 && skipNum > 0) { // Some basic logging every 5000 rows.
					logBasic("Cmis Input - Read " + (skipNum + 1) + " rows.");
				}

				++itemInPage;
				++skipNum;
			}
		} while (results.getHasMoreItems());

		logDebug("Cmis Input - Ending...");
		return true;

	}

	private Object[] buildRowData(QueryResult result, int[] outputRowIndex) throws KettleException {
		Object[] outputRow = RowDataUtil.allocateRowData(result.getProperties().size());
		for(int j=0; j < result.getProperties().size(); ++j) {

			PropertyData<?> property = result.getProperties().get(j);

			Object propertyValue = null;
			if (property.getValues().size() == 0) {
				if (outputRowIndex[j] > 0)
					throw new KettleException("Requested the property '" + property.getQueryName() + "' at the index '" + outputRowIndex[j] + "' on an empty property.");
			}
			else {
				if (property.getValues().size() <= outputRowIndex[j])
					throw new KettleException("Property '" + property.getQueryName() + "' has not the multivalue n." + outputRowIndex[j] + ".");
				propertyValue = property.getValues().get(outputRowIndex[j]);
			}

			logRowlevel("Cmis Input - Property: '" + property.getQueryName() + "'='" + propertyValue + "'.");
			if (propertyValue == null || propertyValue.equals(""))
				outputRow[j] = null;
			else if (property.getClass().isInstance(new PropertyDateTimeImpl()))
				outputRow[j] = new Date(((GregorianCalendar) propertyValue).getTimeInMillis());
			else if (property.getClass().isInstance(new PropertyIntegerImpl()))
				outputRow[j] = ((BigInteger)propertyValue).longValue();
			else
				outputRow[j] = propertyValue;
		}
		return outputRow;
	}

	private int[] getNextRowDataIndex(QueryResult result, int[] outputRowIndex) {
		for(int j=0; j < result.getProperties().size(); ++j) {

			PropertyData<?> property = result.getProperties().get(j);

			if (property.getValues().size() > (outputRowIndex[j] + 1)) {
				outputRowIndex[j] = outputRowIndex[j] + 1;
				return outputRowIndex;
			}
		}
		return null;
	}

	private int[] allocateRowDataIndex(int size) {
		int[] rowDataIndex = new int[size];
		for(int j=0; j < size; ++j)
			rowDataIndex[j] = 0;
		return rowDataIndex;
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
