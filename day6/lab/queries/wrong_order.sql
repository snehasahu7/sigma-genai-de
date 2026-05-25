SELECT department, COUNT(*)
FROM employees
ORDER BY department
WHERE salary > 50000
HAVING COUNT(*) > 5
GROUP BY department;