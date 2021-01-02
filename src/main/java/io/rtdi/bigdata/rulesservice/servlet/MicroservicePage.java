package io.rtdi.bigdata.rulesservice.servlet;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

@WebServlet("/ui5/Microservice")
public class MicroservicePage extends UI5ServletAbstract {
	private static final long serialVersionUID = 684367432L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public MicroservicePage() {
        super("Rule Microservice", "Microservice");
    }

}
