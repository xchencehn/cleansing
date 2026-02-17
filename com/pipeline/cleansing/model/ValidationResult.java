package com.pipeline.cleansing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 算子参数验证结果
 */
public class ValidationResult implements Serializable {
    private boolean valid;
    private List<String> errors;
    private List<String> warnings;

    public ValidationResult() {
        this.valid = true;
        this.errors = new ArrayList<>();
        this.warnings = new ArrayList<>();
    }

    public static ValidationResult success() {
        return new ValidationResult();
    }

    public static ValidationResult failure(String error) {
        ValidationResult result = new ValidationResult();
        result.addError(error);
        return result;
    }

    public void addError(String error) {
        this.errors.add(error);
        this.valid = false;
    }

    public void addWarning(String warning) {
        this.warnings.add(warning);
    }

    public boolean isValid() { return valid; }
    public List<String> getErrors() { return errors; }
    public List<String> getWarnings() { return warnings; }
}
