package com.oron.wallet.financialrecon.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReconController {

    @PostMapping("/recon")
    public ResponseEntity<String> processRecon() {
        String successResponse = "Recon process was completed successfully.";
        return new ResponseEntity<>(successResponse, HttpStatus.OK);
    }

}
