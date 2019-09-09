package com.wejo.seasonality;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


@Controller
public class HomeController {
	
	private static final Logger logger = LoggerFactory.getLogger(HomeController.class);
	
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String home(Locale locale, Model model) {
		DBCommon dbCommon = new DBCommon("finalseasonalitydata3");
		model.addAttribute("select_result", dbCommon.selectDataTableTag(new Seasonality()));
		System.out.println("작동확인");
		return "list";
	}

}
