package org.apache.beam.examples;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@RequiredArgsConstructor
@EqualsAndHashCode
@Getter
public class Result implements Serializable {
    private final String id;
    private final String payload;
}
