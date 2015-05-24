package eap.config.module.error.clr;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import eap.base.BaseController;
import eap.config.module.P;

@Controller
public class ErrorCLR extends BaseController {
	
	@RequestMapping(value=P.ERROR_PAGE_404)
	public String _404() {
		return "404";
	}
	
	@RequestMapping(value=P.ERROR_PAGE_409)
	public String _409() {
		return "409";
	}
	
	@RequestMapping(value=P.ERROR_PAGE_500)
	public String _500(HttpServletRequest request, HttpServletResponse response) {
		response.setStatus(500);
		return "500";
	}
}