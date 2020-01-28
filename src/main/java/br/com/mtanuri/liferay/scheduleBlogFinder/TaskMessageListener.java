package br.com.mtanuri.liferay.scheduleBlogFinder;

import com.liferay.blogs.model.BlogsEntry;
import com.liferay.blogs.service.BlogsEntryLocalService;
import com.liferay.expando.kernel.model.ExpandoColumn;
import com.liferay.expando.kernel.model.ExpandoValue;
import com.liferay.expando.kernel.service.ExpandoColumnLocalServiceUtil;
import com.liferay.expando.kernel.service.ExpandoValueLocalServiceUtil;
import com.liferay.portal.kernel.dao.orm.DynamicQuery;
import com.liferay.portal.kernel.dao.orm.DynamicQueryFactoryUtil;
import com.liferay.portal.kernel.dao.orm.RestrictionsFactoryUtil;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.messaging.BaseMessageListener;
import com.liferay.portal.kernel.messaging.DestinationNames;
import com.liferay.portal.kernel.messaging.Message;
import com.liferay.portal.kernel.model.Company;
import com.liferay.portal.kernel.model.Role;
import com.liferay.portal.kernel.model.User;
import com.liferay.portal.kernel.module.framework.ModuleServiceLifecycle;
import com.liferay.portal.kernel.scheduler.SchedulerEngineHelper;
import com.liferay.portal.kernel.scheduler.SchedulerEntryImpl;
import com.liferay.portal.kernel.scheduler.SchedulerException;
import com.liferay.portal.kernel.scheduler.StorageType;
import com.liferay.portal.kernel.scheduler.StorageTypeAware;
import com.liferay.portal.kernel.scheduler.Trigger;
import com.liferay.portal.kernel.scheduler.TriggerFactory;
import com.liferay.portal.kernel.security.permission.PermissionChecker;
import com.liferay.portal.kernel.security.permission.PermissionCheckerFactoryUtil;
import com.liferay.portal.kernel.security.permission.PermissionThreadLocal;
import com.liferay.portal.kernel.service.CompanyLocalService;
import com.liferay.portal.kernel.service.RoleLocalServiceUtil;
import com.liferay.portal.kernel.service.UserLocalServiceUtil;
import com.liferay.portal.kernel.util.GetterUtil;
import com.liferay.portal.kernel.util.PortalClassLoaderUtil;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;

@Component(immediate = true, property = { "cron.expression=0 * * ? * *	" }, service = TaskMessageListener.class)
public class TaskMessageListener extends BaseMessageListener {

	@Reference
	private BlogsEntryLocalService blogsEntryLocalService;

	@Reference
	private CompanyLocalService companyLocalService;

	/**
	 * doReceive: This is where the magic happens, this is where you want to do the
	 * work for the scheduled job.
	 * 
	 * @param message This is the message object tied to the job. If you stored data
	 *                with the job, the message will contain that data.
	 * @throws Exception In case there is some sort of error processing the task.
	 */
	@Override
	protected void doReceive(Message message) throws Exception {

		Calendar calendar = Calendar.getInstance();
		boolean is30MinuteInterval = calendar.get(Calendar.MINUTE) == 0 || calendar.get(Calendar.MINUTE) == 30;

		if (!is30MinuteInterval) {
			return;
		}

		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		this.setPermissionChecker();

		_log.info("Listing blogs entries...");
		List<BlogsEntry> blogsEntries = this.getBlogsScheduledFor(calendar.getTimeInMillis());
		if (!blogsEntries.isEmpty()) {
			for (BlogsEntry blogsEntry : blogsEntries) {
				_log.info(blogsEntry.getTitle());
				_log.info(blogsEntry.getExpandoBridge().getAttribute("Push Notification"));
				_log.info("********************");
			}
		} else {
			_log.info("No blogs scheduled for this moment: " + calendar);
		}
	}

	private void setPermissionChecker() throws PortalException {
		Company company = this.getCompany();
		Role adminRole = RoleLocalServiceUtil.getRole(company.getCompanyId(), "Administrator");
		List<User> adminUsers = UserLocalServiceUtil.getRoleUsers(adminRole.getRoleId());

		PermissionChecker permissionChecker = PermissionCheckerFactoryUtil.create(adminUsers.get(0));
		PermissionThreadLocal.setPermissionChecker(permissionChecker);
	}

	public List<BlogsEntry> getBlogsScheduledFor(Long timeInMillis) {
		List<BlogsEntry> blogEntries = new ArrayList<BlogsEntry>();

		try {
			Company company = this.getCompany();
			String customFieldName = "Push Notification";
			ExpandoColumn column = ExpandoColumnLocalServiceUtil.getDefaultTableColumn(company.getCompanyId(),
					BlogsEntry.class.getName(), customFieldName);
			if (column != null) {
				ClassLoader cl = PortalClassLoaderUtil.getClassLoader();
				DynamicQuery query = DynamicQueryFactoryUtil.forClass(ExpandoValue.class, cl);
				query.add(RestrictionsFactoryUtil.eq("tableId", column.getTableId()));
				query.add(RestrictionsFactoryUtil.eq("columnId", column.getColumnId()));
				query.add(RestrictionsFactoryUtil.ilike("data", "%" + timeInMillis + "%"));
				List<ExpandoValue> results = ExpandoValueLocalServiceUtil.dynamicQuery(query);
				for (ExpandoValue expandoValue : results) {
					BlogsEntry entry = blogsEntryLocalService.getBlogsEntry(expandoValue.getClassPK());
					blogEntries.add(entry);
				}
				return blogEntries;

			} else {
				_log.warn("DynamicQuery for BlogsEntry has encounter a problem: Custom Field '" + customFieldName
						+ "' doesn't exist.");

			}
			return null;
		} catch (Exception e) {
			return null;
		}
	}

	private Company getCompany() throws PortalException {
		return companyLocalService.getCompanyByWebId("liferay.com");
	}

	/**
	 * activate: Called whenever the properties for the component change (ala Config
	 * Admin) or OSGi is activating the component.
	 * 
	 * @param properties The properties map from Config Admin.
	 * @throws SchedulerException in case of error.
	 */
	@Activate
	@Modified
	protected void activate(Map<String, Object> properties) throws SchedulerException {

		// extract the cron expression from the properties
		String cronExpression = GetterUtil.getString(properties.get("cron.expression"), _DEFAULT_CRON_EXPRESSION);

		// create a new trigger definition for the job.
		String listenerClass = getClass().getName();
		Trigger jobTrigger = _triggerFactory.createTrigger(listenerClass, listenerClass, new Date(), null,
				cronExpression);

		// wrap the current scheduler entry in our new wrapper.
		// use the persisted storaget type and set the wrapper back to the class field.
		_schedulerEntryImpl = new SchedulerEntryImpl(getClass().getName(), jobTrigger);
		_schedulerEntryImpl = new StorageTypeAwareSchedulerEntryImpl(_schedulerEntryImpl, StorageType.PERSISTED);

		// update the trigger for the scheduled job.
		_schedulerEntryImpl.setTrigger(jobTrigger);

		// if we were initialized (i.e. if this is called due to CA modification)
		if (_initialized) {
			// first deactivate the current job before we schedule.
			deactivate();
		}

		// register the scheduled task
		_schedulerEngineHelper.register(this, _schedulerEntryImpl, DestinationNames.SCHEDULER_DISPATCH);

		// set the initialized flag.
		_initialized = true;
	}

	/**
	 * deactivate: Called when OSGi is deactivating the component.
	 */
	@Deactivate
	protected void deactivate() {
		// if we previously were initialized
		if (_initialized) {
			// unschedule the job so it is cleaned up
			try {
				_schedulerEngineHelper.unschedule(_schedulerEntryImpl, getStorageType());
			} catch (SchedulerException se) {
				if (_log.isWarnEnabled()) {
					_log.warn("Unable to unschedule trigger", se);
				}
			}

			// unregister this listener
			_schedulerEngineHelper.unregister(this);
		}

		// clear the initialized flag
		_initialized = false;
	}

	/**
	 * getStorageType: Utility method to get the storage type from the scheduler
	 * entry wrapper.
	 * 
	 * @return StorageType The storage type to use.
	 */
	protected StorageType getStorageType() {
		if (_schedulerEntryImpl instanceof StorageTypeAware) {
			return ((StorageTypeAware) _schedulerEntryImpl).getStorageType();
		}

		return StorageType.MEMORY_CLUSTERED;
	}

	/**
	 * setModuleServiceLifecycle: So this requires some explanation...
	 * 
	 * OSGi will start a component once all of it's dependencies are satisfied.
	 * However, there are times where you want to hold off until the portal is
	 * completely ready to go.
	 * 
	 * This reference declaration is waiting for the ModuleServiceLifecycle's
	 * PORTAL_INITIALIZED component which will not be available until, surprise
	 * surprise, the portal has finished initializing.
	 * 
	 * With this reference, this component activation waits until portal
	 * initialization has completed.
	 * 
	 * @param moduleServiceLifecycle
	 */
	@Reference(target = ModuleServiceLifecycle.PORTAL_INITIALIZED, unbind = "-")
	protected void setModuleServiceLifecycle(ModuleServiceLifecycle moduleServiceLifecycle) {
	}

	@Reference(unbind = "-")
	protected void setTriggerFactory(TriggerFactory triggerFactory) {
		_triggerFactory = triggerFactory;
	}

	@Reference(unbind = "-")
	protected void setSchedulerEngineHelper(SchedulerEngineHelper schedulerEngineHelper) {
		_schedulerEngineHelper = schedulerEngineHelper;
	}

	// the default cron expression is to run daily at midnight
	private static final String _DEFAULT_CRON_EXPRESSION = "0 0 0 * * ?";

	private static final Log _log = LogFactoryUtil.getLog(TaskMessageListener.class);

	private volatile boolean _initialized;
	private TriggerFactory _triggerFactory;
	private SchedulerEngineHelper _schedulerEngineHelper;
	private SchedulerEntryImpl _schedulerEntryImpl = null;
}