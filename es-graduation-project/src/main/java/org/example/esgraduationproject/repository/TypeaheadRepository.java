package org.example.esgraduationproject.repository;

import org.example.esgraduationproject.model.TypeaheadServiceRequest;
import org.example.esgraduationproject.model.TypeaheadServiceResponse;

public interface TypeaheadRepository {
    TypeaheadServiceResponse getAllTypeaheads(TypeaheadServiceRequest request);
    TypeaheadServiceResponse getTypeaheadsByQuery(TypeaheadServiceRequest request);

    void recreateIndex();
}
