package io.rtdi.bigdata.rulesservice.servlet;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

@WebServlet("/ui5/ServiceDetails")
public class ServiceDetailsPage extends UI5ServletAbstract {
	private static final long serialVersionUID = 68436742432L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ServiceDetailsPage() {
        super("Rule Configuration", "Ruleset");
    }

}
