package org.example.esgraduationproject.rest;

import org.example.esgraduationproject.model.TypeaheadServiceRequest;
import org.example.esgraduationproject.model.TypeaheadServiceResponse;
import org.example.esgraduationproject.service.TypeaheadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/v1/typeahead")
public class TypeaheadController {

    @Autowired
    private TypeaheadService typeaheadService;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
    public TypeaheadServiceResponse getSearchServiceResponse(@RequestBody TypeaheadServiceRequest request) {
        return typeaheadService.getServiceResponse(request);
    }
}
