package it.francescocorti.kettle.cmisinput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * @author Francesco Corti
 * @since 2014-04-10
 * @version 1.2
 * @see http://fcorti.com
 */
public class CmisInputDialog extends BaseStepDialog implements StepDialogInterface
{
	private CmisInputMeta input;
	private String url;
	private String login;
	private String password;
	private String cmisQuery;

	private TextVar wUrl;
	private TextVar wLogin;
	private TextVar wPassword;
	private Text wCmisQuery;

	public CmisInputDialog(Shell parent,Object in,TransMeta transMeta,String sname)
	{
		super(parent,(BaseStepMeta) in,transMeta,sname);
		input = (CmisInputMeta) in;
		if (input.getUrl().isEmpty())
		{
			input.setDefault();
		}
		url = input.getUrl();
		login = input.getLogin();
		password = input.getPassword();
		cmisQuery = input.getCmisQuery();
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();
		
		shell = new Shell(parent,SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
        setShellImage(shell,input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(Messages.getString("CmisInputDialog.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// StepName line
		wlStepname = new Label(shell,SWT.RIGHT);
		wlStepname.setText(Messages.getString("CmisInputDialog.StepName.Label"));
        props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0,0);
		fdlStepname.right = new FormAttachment(middle,-margin);
		fdlStepname.top = new FormAttachment(0,margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell,SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
        props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle,0);
		fdStepname.right = new FormAttachment(100,0);
		fdStepname.top = new FormAttachment(0,margin);
		wStepname.setLayoutData(fdStepname);

		// Url line
		Label wlUrl = new Label(shell,SWT.RIGHT);
		wlUrl.setText(Messages.getString("CmisInputDialog.Url.Label"));
        props.setLook(wlUrl);
		FormData fdlUrl = new FormData();
		fdlUrl.left = new FormAttachment(0,0);
		fdlUrl.right = new FormAttachment(middle,-margin);
		fdlUrl.top = new FormAttachment(wStepname,margin);
		wlUrl.setLayoutData(fdlUrl);
		wUrl = new TextVar(transMeta,shell,SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wUrl);
		wUrl.addModifyListener(lsMod);
		wUrl.setToolTipText("CmisInputDialog.Url.Tooltip"); 
		FormData fdUrl = new FormData();
		fdUrl.left = new FormAttachment(middle,0);
		fdUrl.top = new FormAttachment(wStepname,margin);
		fdUrl.right = new FormAttachment(100,0);
		wUrl.setLayoutData(fdUrl);

		// Login line
		Label wlLogin = new Label(shell,SWT.RIGHT);
		wlLogin.setText(Messages.getString("CmisInputDialog.Login.Label"));
        props.setLook(wlLogin);
		FormData fdlLogin = new FormData();
		fdlLogin.left = new FormAttachment(0,0);
		fdlLogin.right = new FormAttachment(middle,-margin);
		fdlLogin.top = new FormAttachment(wUrl,margin);
		wlLogin.setLayoutData(fdlLogin);
		wLogin = new TextVar(transMeta,shell,SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wLogin);
		wLogin.addModifyListener(lsMod);
		wLogin.setToolTipText("CmisInputDialog.Login.Tooltip"); 
		FormData fdLogin = new FormData();
		fdLogin.left = new FormAttachment(middle,0);
		fdLogin.right = new FormAttachment(100,0);
		fdLogin.top = new FormAttachment(wUrl,margin);
		wLogin.setLayoutData(fdLogin);

		// Password line
		Label wlPassword = new Label(shell,SWT.RIGHT);
		wlPassword.setText(Messages.getString("CmisInputDialog.Password.Label"));
        props.setLook(wlPassword);
		FormData fdlPassword = new FormData();
		fdlPassword.left = new FormAttachment(0,0);
		fdlPassword.right = new FormAttachment(middle,-margin);
		fdlPassword.top = new FormAttachment(wLogin,margin);
		wlPassword.setLayoutData(fdlPassword);
		wPassword = new TextVar(transMeta,shell,SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wPassword);
		wPassword.addModifyListener(lsMod);
		wPassword.setToolTipText("CmisInputDialog.Password.Tooltip"); 
		FormData fdPassword = new FormData();
		fdPassword.left = new FormAttachment(middle,0);
		fdPassword.right = new FormAttachment(100,0);
		fdPassword.top = new FormAttachment(wLogin,margin);
		wPassword.setLayoutData(fdPassword);

		// Query line
		Label wlCmisQuery = new Label(shell,SWT.RIGHT);
		wlCmisQuery.setText(Messages.getString("CmisInputDialog.CmisQuery.Label"));
        props.setLook(wlCmisQuery);
		FormData fdlCmisQuery = new FormData();
		fdlCmisQuery.left = new FormAttachment(0,0);
		fdlCmisQuery.right = new FormAttachment(middle,-margin);
		fdlCmisQuery.top = new FormAttachment(wPassword,margin);
		wlCmisQuery.setLayoutData(fdlCmisQuery);
		wCmisQuery = new Text(shell,SWT.MULTI | SWT.LEFT | SWT.BORDER);
		props.setLook(wCmisQuery);
		wCmisQuery.addModifyListener(lsMod);
		wCmisQuery.setToolTipText("CmisInputDialog.CmisQuery.Tooltip"); 
		FormData fdCmisQuery = new FormData();
		fdCmisQuery.left = new FormAttachment(middle,0);
		fdCmisQuery.right = new FormAttachment(100,0);
		fdCmisQuery.top = new FormAttachment(wPassword,margin);
		fdCmisQuery.bottom = new FormAttachment(80,0);
		wCmisQuery.setLayoutData(fdCmisQuery);

		// Some buttons
		wOK = new Button(shell,SWT.PUSH);
		wOK.setText(Messages.getString("System.Button.OK"));
		wCancel = new Button(shell,SWT.PUSH);
		wCancel.setText(Messages.getString("System.Button.Cancel"));

        BaseStepDialog.positionBottomButtons(shell,new Button[] {wOK,wCancel},margin,wCmisQuery);

		// Add listeners
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();} };

		wCancel.addListener(SWT.Selection,lsCancel);
		wOK.addListener    (SWT.Selection,lsOK);

		lsDef = new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };

		wStepname.addSelectionListener(lsDef);
		wUrl.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } });

		// Set the shell size, based upon previous time...
		setSize();

		getData();
		input.setChanged(changed);
	
		shell.open();
		while (!shell.isDisposed())
		{
		    if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	
	// Read data from input (TextFileInputInfo)
	public void getData()
	{
		wStepname.selectAll();
		if (url != null)
		{
			wUrl.setText(url);
		}
		if (login != null)
		{
			wLogin.setText(login);
		}
		if (password != null)
		{
			wPassword.setText(password);
		}
		if (cmisQuery != null)
		{
			wCmisQuery.setText(cmisQuery);
		}
	}

	private void cancel()
	{
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void ok()
	{
		stepname = wStepname.getText();
		url = wUrl.getText();
		login = wLogin.getText();
		password = wPassword.getText();
		cmisQuery = wCmisQuery.getText();
		input.setUrl(url);
		input.setLogin(login);
		input.setPassword(password);
		input.setCmisQuery(cmisQuery);
		dispose();
	}
}
