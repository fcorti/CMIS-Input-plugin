package it.francescocorti.kettle.cmisinput;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisInputData extends BaseStepData implements StepDataInterface
{
	public RowMetaInterface outputRowMeta;

    public CmisInputData()
	{
		super();
	}
}
