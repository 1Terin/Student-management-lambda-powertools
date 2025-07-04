import { z } from 'zod';
import { search } from '@jmespath-community/jmespath';
import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';

const processedRequests = new Set<string>();

const StudentSchema = z.object({
  name: z.string(),
  Subject: z.object({
    science: z.number().min(0).max(100),
    maths: z.number().min(0).max(100),
    result: z.enum(['pass', 'fail']),
  }),
  Attendance: z.number().min(0).max(100),
});

const InputSchema = z.object({
  result: z.array(StudentSchema),
});

export const handler = async (event: APIGatewayProxyEvent, context: Context): Promise<APIGatewayProxyResult> => {
  try {
    console.log(JSON.stringify(event, null, 2));

    let body: unknown;
    try {
      body = JSON.parse(event.body || '{}');
    } catch (err) {
      console.error(err);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Invalid JSON input' }),
      };
    }

    const validatedData = InputSchema.safeParse(body);
    if (!validatedData.success) {
      console.error(validatedData.error);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: 'Invalid input', details: validatedData.error.errors }),
      };
    }

    const students = validatedData.data.result;

    const idempotencyKey: string = event.requestContext?.requestId || context.awsRequestId;
    if (processedRequests.has(idempotencyKey)) {
      console.log(`Idempotent request detected for key: ${idempotencyKey}. Skipping processing.`);
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Request already processed' }),
      };
    }
    processedRequests.add(idempotencyKey);
    console.log(`Processing new request with idempotency key: ${idempotencyKey}`);

    const results = {
      studentNames: search(students, `[*].name`),
      scienceMarks: search(students, `[*].Subject.science`),
      scienceAbove80: search(students, `[?Subject.science > \`80\`].name`),
      passedStudents: search(students, `[?Subject.result == 'pass'].name`),
      passedLowAttendance: search(students, `[?Subject.result == 'pass' && Attendance < \`50\`].name`),
      perfectScore: search(students, `[?Subject.science == \`100\` || Subject.maths == \`100\`].name`),
      nameAndResult: search(students, `[*].{Name: name, Result: Subject.result}`),
    };

    console.log('All JMESPath operations completed successfully.');

    return {
      statusCode: 200,
      body: JSON.stringify(results),
      headers: {
        'Content-Type': 'application/json',
      },
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Internal server error', message: (err as Error).message }),
      headers: {
        'Content-Type': 'application/json',
      },
    };
  }
};