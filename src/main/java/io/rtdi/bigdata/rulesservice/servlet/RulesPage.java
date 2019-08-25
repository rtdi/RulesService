package io.rtdi.bigdata.rulesservice.servlet;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

@WebServlet("/ui5/Ruleset")
public class RulesPage extends UI5ServletAbstract {
	private static final long serialVersionUID = 68436742432L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public RulesPage() {
        super("Rule Configuration", "Ruleset");
    }

}
