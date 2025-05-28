package org.example.esgraduationproject.service;

import org.example.esgraduationproject.model.TypeaheadServiceRequest;
import org.example.esgraduationproject.model.TypeaheadServiceResponse;

public interface TypeaheadService {
    TypeaheadServiceResponse getServiceResponse(TypeaheadServiceRequest request);

    void recreateIndex();
}
