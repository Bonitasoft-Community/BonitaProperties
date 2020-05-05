package org.bonitasoft.properties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bonitasoft.engine.api.IdentityAPI;
import org.bonitasoft.engine.api.ProcessAPI;
import org.bonitasoft.engine.api.ProfileAPI;
import org.bonitasoft.engine.identity.Group;
import org.bonitasoft.engine.identity.Role;
import org.bonitasoft.engine.identity.User;
import org.bonitasoft.engine.identity.UserMembership;
import org.bonitasoft.engine.identity.UserMembershipCriterion;
import org.bonitasoft.engine.profile.Profile;
import org.bonitasoft.engine.profile.ProfileCriterion;
import org.bonitasoft.engine.session.APISession;
import org.bonitasoft.log.event.BEvent;
import org.bonitasoft.log.event.BEventFactory;

import com.google.gson.Gson;

/**
 * this class implement the access to the BonitaProperties.
 * Is an user can access the properties (in read, write ? )
 *
 * @author Firstname Lastname
 */

public class AccessProperties {

    private static Logger logger = Logger.getLogger(AccessProperties.class.getName());
    private final String loggerLabel = "BonitaProperties_2.3.0:";

    private Long mTenantId;

    public static class Rule {

        /**
         * optional : you can give a name
         */
        public String ruleName;
        /**
         * Name : on with name this rule apply
         */
        public String name;
        /**
         * Domain : on with domain this rule apply
         */
        public String domain;
        /**
         * true: only a read access.
         */
        public boolean isReadOnly;
        /**
         * if true, the only access is when the domainName equals the userId
         */
        public Boolean domainIsUserId;
        /**
         * if the user is an administrator, he can access this name / domain
         */
        public Boolean isAdmin;

        /**
         * if True, then the rule apply for the root and apply on all domain
         */
        public Boolean applyAllDomain;

        /**
         * can access if the user has a membership in this role
         */
        public String roleName;
        /**
         * can access if the user has a memebership in this group
         */
        public String groupPath;
        /**
         * can access only if the user has a membership in this profile
         */
        public String profileName;
        /**
         * can access if the user is this one
         */
        public String userName;

        public Map<String, Object> toMap() {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put("ruleName", ruleName);
            map.put("name", name);
            map.put("domain", domain);
            map.put("isReadOnly", isReadOnly);
            map.put("domainIsUserId", domainIsUserId);
            map.put("isAdmin", isAdmin);
            map.put("applyAllDomain", applyAllDomain);
            map.put("roleName", roleName);
            map.put("groupPath", groupPath);
            map.put("profileName", profileName);
            map.put("userName", userName);
            return map;
        }

        public String toJson() {
            final Gson gson = new Gson();
            final String json = gson.toJson(this); // serializes target to Json
            return json;
        }

        public static Rule fromJson(final String json) {
            final Gson gson = new Gson();

            final Rule rule = gson.fromJson(json, Rule.class);
            return rule;
        }

        /**
         * check if the rule is valid : should have monimum some parameters
         *
         * @return
         */
        public boolean isValid() {
            if (isEmpty(name)) {
                return false;
            }

            return Boolean.TRUE.equals(domainIsUserId)
                    || Boolean.TRUE.equals(isAdmin)
                    || Boolean.TRUE.equals(applyAllDomain)
                    || !isEmpty(roleName)
                    || !isEmpty(groupPath)
                    || !isEmpty(profileName)
                    || !isEmpty(userName);

        }

        public String getError() {
            return "A Name [name] must be define and then one of these parameters : [domainIsUserId,isAdmin,roleName,groupPath]";
        }

        @Override
        public String toString() {
            return "Rule [" + (ruleName == null ? "" : ruleName) + "] "
                    + " Name[" + name
                    + "+ domain[" + domain
                    + "] " + (isReadOnly ? "r" : "r/w")
                    + " DomainUserId:" + domainIsUserId
                    + " isAdmin=" + isAdmin
                    + " applyAllDomain=" + applyAllDomain
                    + " roleName[" + roleName
                    + "] groupPath[" + groupPath
                    + "] profileName[" + profileName
                    + "] userName[" + userName + "]";
        }

        /**
         * to order the display, return a key based on name + domain + rule
         *
         * @return
         */
        public String getKeyToOrder() {
            return getFormattedAttribute(name) + getFormattedAttribute(domain) + getFormattedAttribute(ruleName);
        }
    }

    public AccessProperties(final long tenantId) {
        mTenantId = tenantId;
    }

    /**
     * is Admin ?
     * Is the user part of the ADMINISTRATOR profile
     *
     * @param apiSession
     * @param profileAPI
     * @return
     */
    public boolean isAdmin(final APISession apiSession, final ProfileAPI profileAPI) {
        mTenantId = apiSession.getTenantId();
        final List<Profile> listProfiles = profileAPI.getProfilesForUser(apiSession.getUserId(), 0, 10000,
                ProfileCriterion.NAME_ASC);
        for (final Profile profile : listProfiles) {
            if (profile.getName().equals("Administrator")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check the permission to access this properties.
     * To verify : check if there are some rule for this name. If no rule exist, then it's allowed. Else, the rule is verified.
     *
     * @param name
     * @param domain
     * @param userSession
     * @param processAPI
     * @param identityAPI
     * @return
     */
    public boolean isAccepted(final String name, final String domain, final boolean writeAccess,
            final APISession apiSession,
            final ProcessAPI processAPI,
            final IdentityAPI identityAPI, final ProfileAPI profileAPI) {
        mTenantId = apiSession.getTenantId();

        // only administrator can access this properties !
        if (name.equals(cstNameRuleProperties)) {
            return isAdmin(apiSession, profileAPI);
        }

        final RulesResult ruleResult = getRules();
        if (BEventFactory.isError(ruleResult.listEvents)) {
            return false;
        }
        // extract the rule for this name
        final List<Rule> listRulesForThisName = new ArrayList<Rule>();
        for (final Rule rule : ruleResult.listRules) {
            if (rule.name.equalsIgnoreCase(name)) {
                listRulesForThisName.add(rule);
            }
        }
        logger.info("AccessProperties.isAccepted: Nb rule for[" + name + "] domain[" + domain + "] ["
                + listRulesForThisName.size() + "] : if (0=> accepted)");
        if (listRulesForThisName.size() == 0) {
            return true; // no rule, access allowed
        }
        try {
            final User user = identityAPI.getUser(apiSession.getUserId());

            for (final Rule rule : listRulesForThisName) {
                String debugRule = "";
                debugRule += " ReadOnly=" + rule.isReadOnly + ",writeAccess=" + writeAccess;
                if (rule.isReadOnly && writeAccess) {
                    logger.info("AccessProperties.rule: Rule[" + rule.ruleName + "] " + debugRule);
                    continue;
                }
                String acceptThisRule = "";

                //                             domain=null      domain=sfo     domain=663
                //   applyAllDomain =>          true             true           true
                //      (we don't care all)
                //   ! applyAllDomain -----------------------------------------------
                //          rule.domainIsUserId    false          false         true
                //          rule.domain=sfo        false          true          false
                debugRule += ";applyAllDomain=" + rule.applyAllDomain;
                if (rule.applyAllDomain != null && rule.applyAllDomain) {
                    // don't check any rule according the domain
                    debugRule += ":Allow;";
                } else {
                    // check now domainIsUserId
                    debugRule += ";domainIsUserId=" + rule.domainIsUserId;

                    if (rule.domainIsUserId != null && rule.domainIsUserId) {
                        debugRule = ";domain[" + domain + "] userId[" + apiSession.getUserId() + "]";
                        if (domain == null || !domain.equals(String.valueOf(apiSession.getUserId()))) {
                            debugRule += ":rejected;";
                            acceptThisRule += "rule.domainIsUserId and domain is different domain[" + domain
                                    + "] userId["
                                    + String.valueOf(apiSession.getUserId()) + ";";
                        } else {
                            debugRule += ":accepted;";
                        }
                    } else {
                        debugRule += ";CheckDomain[" + domain + "] rule.domain[" + rule.domain + "]";

                        // check domain
                        if (domain != null) {
                            debugRule += "Domain[" + domain + "] - rule.domain[" + rule.domain + "]";
                            if (rule.domain == null || !domain.equals(rule.domain)) {
                                debugRule += ":rejected;";
                                acceptThisRule += "Rule not apply to the domain[" + domain + "] (rule.domain ["
                                        + rule.domain + "]);";
                            } else {
                                debugRule += ":accepted;";
                            }

                        } else {
                            debugRule += ":accepted;";
                            if (rule.domain != null) {
                                debugRule += "Domain is null and rule.domain[" + rule.domain + "];rejected";
                                acceptThisRule += "No domain, and rule is on a domain (rule.domain [" + rule.domain
                                        + "]);";
                            } else {
                                debugRule += ":accepted";
                            }
                        }
                    }
                }

                // profile and admin
                final List<Profile> listProfiles = profileAPI.getProfilesForUser(apiSession.getUserId(), 0, 10000,
                        ProfileCriterion.NAME_ASC);
                boolean isRegisterInAdminProfile = false;
                boolean isRegisterInRuleProfile = false;
                for (final Profile profile : listProfiles) {
                    if (profile.getName().equals("Administrator")) {
                        isRegisterInAdminProfile = true;
                    }
                    if (!isEmpty(rule.profileName) && profile.getName().equals(rule.profileName)) {
                        isRegisterInRuleProfile = true;
                    }
                }
                if (!isEmpty(rule.profileName) && !isRegisterInRuleProfile) {
                    acceptThisRule += "rule.profileName : user is not in profile[" + rule.profileName + "];";
                }

                debugRule += ";isAdmin ? " + rule.isAdmin + " / isRegisterInAdminProfile=" + isRegisterInAdminProfile;
                if (rule.isAdmin != null && rule.isAdmin && !isRegisterInAdminProfile) {
                    acceptThisRule += "rule.isAdmin : not in Administrator profile;";
                }

                // role & group
                boolean isRegisterInRuleRole = false;
                boolean isRegisterInRuleGroup = false;
                final List<UserMembership> listMemberShip = identityAPI.getUserMemberships(apiSession.getUserId(), 0,
                        10000,
                        UserMembershipCriterion.ROLE_NAME_ASC);
                final Role role = isEmpty(rule.roleName) ? null : identityAPI.getRoleByName(rule.roleName);
                final Group group = isEmpty(rule.groupPath) ? null : identityAPI.getGroupByPath(rule.groupPath);
                for (final UserMembership userMemberShip : listMemberShip) {
                    if (role != null && userMemberShip.getRoleId() == role.getId()) {
                        isRegisterInRuleRole = true;
                    }
                    if (group != null && userMemberShip.getGroupId() == group.getId()) {
                        isRegisterInRuleGroup = true;
                    }
                }
                if (!isEmpty(rule.roleName) && !isRegisterInRuleRole) {
                    acceptThisRule += "rule.roleName : not register in role[" + rule.roleName + "]";
                }
                if (!isEmpty(rule.groupPath) && !isRegisterInRuleGroup) {
                    acceptThisRule += "rule.groupName : not register in group[" + rule.groupPath + "]";
                }

                // userName
                if (!isEmpty(rule.userName) && !rule.userName.equals(user.getUserName())) {
                    acceptThisRule += "rule.userName : userName[" + user.getUserName() + "] does not match ruleuser["
                            + rule.userName + "];";
                }

                logger.info("AccessProperties.rule: Rule[" + rule.ruleName + "] " + debugRule);

                if (acceptThisRule.length() == 0) {
                    logger.info("AccessProperties.isAccepted: one rule [" + rule.ruleName + "] ACCEPTED");
                    return true;
                } else {
                    logger.info(
                            "AccessProperties.isAccepted: rule [" + rule.ruleName + "] not accepted " + acceptThisRule);
                }

            }
        } catch (final Exception e) {
            logger.severe("AccessProperties.isAccepted Exception during isAccepted " + e.toString());
        }
        return false;
    }

    public static String cstNameRuleProperties = "BonitaPropertiesAccess";

    /**
     * @return
     */
    public class RulesResult {

        public List<Rule> listRules = new ArrayList<Rule>();
        public List<BEvent> listEvents;
    }

    public RulesResult getRules() {
        final RulesResult rulesResult = new RulesResult();

        final BonitaProperties bonitaProperties = new BonitaProperties(cstNameRuleProperties, mTenantId);
        bonitaProperties.setCheckDatabase(false);
        rulesResult.listEvents = bonitaProperties.load();

        for (int i = 0; i < 1000; i++) {
            final String ruleJson = bonitaProperties.getProperty("rule_" + i);
            if (ruleJson == null) {
                break;
            }
            final Rule rule = Rule.fromJson(ruleJson);
            rulesResult.listRules.add(rule);
            // logger.info("AccessProperties.getRules : read rule[" + rule.toString());
        }
        return rulesResult;

    }

    /**
     * @param listRules
     * @return
     */
    public List<BEvent> setRules(final List<Rule> listRules) {
        final BonitaProperties bonitaProperties = new BonitaProperties(cstNameRuleProperties, mTenantId);
        bonitaProperties.setCheckDatabase(false);

        bonitaProperties.load();
        bonitaProperties.clear();
        for (int i = 0; i < listRules.size(); i++) {
            logger.info("AccessProperties.setRules : write rule[" + listRules.get(i));

            bonitaProperties.setProperty("rule_" + i, listRules.get(i).toJson());
        }
        final List<BEvent> listEvents = bonitaProperties.store();
        return listEvents;
    }

    public static boolean isEmpty(final String attribut) {
        return attribut == null || attribut.trim().length() == 0;
    }

    public static String getFormattedAttribute(final String attribut) {
        return ((attribut == null ? "" : attribut) + "                                                   ").substring(0,
                50) + "~";
    }

}
